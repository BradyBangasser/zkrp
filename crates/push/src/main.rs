//! zkrp-push: sends push notifications without learning who they're for.
//!
//! Relays hand it sealed tokens (proto/push.proto) when something is deposited
//! in a mailbox. It opens each with its secret key and sends a content-free
//! alert. It never sees mailbox ids, messages, or which relay asked, and it
//! stores and logs nothing about tokens.
//!
//!   zkrp-push keygen <dir>   write push-key (if missing), print PUSH_PUBLIC_KEY
//!   zkrp-push healthcheck    container HEALTHCHECK
//!   zkrp-push                serve gRPC on PUSH_PORT (9003), private network only

use std::path::Path;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use zeroize::Zeroizing;

mod proto {
    tonic::include_proto!("zrp.push.v1");
}

use proto::push_service_server::{PushService, PushServiceServer};
use proto::{NotifyRequest, NotifyResponse};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Tokens per Notify, at most (a relay sends one mailbox's registrations).
const MAX_TOKENS: usize = 32;
const APNS_PRODUCTION: u8 = 1;
const APNS_SANDBOX: u8 = 2;

fn secret(name: &str) -> Result<Zeroizing<Vec<u8>>, BoxError> {
    let dir = std::env::var("CREDENTIALS_DIRECTORY")
        .map_err(|_| format!("CREDENTIALS_DIRECTORY unset; refusing to invent {name}"))?;
    Ok(Zeroizing::new(
        std::fs::read(Path::new(&dir).join(name)).map_err(|e| format!("credential {name}: {e}"))?,
    ))
}

fn env(k: &str) -> Result<String, BoxError> {
    std::env::var(k).map_err(|_| format!("{k} not set").into())
}

/// Write push-key and push-public-key into `dir` unless already there, and
/// print the public key for the relays' PUSH_PUBLIC_KEY.
fn keygen(dir: &Path) -> Result<(), BoxError> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::create_dir_all(dir)?;
    let key_path = dir.join("push-key");
    let secret: [u8; 32] = if key_path.exists() {
        hex::decode(std::fs::read_to_string(&key_path)?.trim())?
            .try_into()
            .map_err(|_| "push-key isn't 32 bytes")?
    } else {
        let (secret, _) = zkcred::sealed_box::keypair();
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        opts.mode(0o600);
        writeln!(opts.open(&key_path)?, "{}", hex::encode(secret))?;
        eprintln!("wrote push-key");
        secret
    };
    let public = hex::encode(zkcred::sealed_box::public_key(&secret));
    std::fs::write(dir.join("push-public-key"), format!("{public}\n"))?;
    println!("PUSH_PUBLIC_KEY={public}");
    Ok(())
}

/// Where alerts go.
enum Sender {
    Apns(Box<Apns>),
    /// Development: say that an alert would go out, never which token.
    Log,
}

struct Apns {
    production: a2::Client,
    sandbox: a2::Client,
    topic: String,
}

struct Push {
    key: Zeroizing<[u8; 32]>,
    sender: Sender,
    title: String,
    body: String,
}

fn record(result: &'static str) {
    telemetry::counter!("zkrp_push_sent_total", "result" => result).increment(1);
}

impl Push {
    async fn send_one(&self, sealed: &[u8]) -> bool {
        let Some(plain) = zkcred::sealed_box::open(&self.key, sealed).map(Zeroizing::new) else {
            record("unopenable");
            return false;
        };
        let Some((&service, token)) = plain.split_first().filter(|(_, t)| !t.is_empty()) else {
            record("malformed");
            return false;
        };
        let (client, topic) = match (&self.sender, service) {
            (Sender::Log, _) => {
                tracing::info!("would push: {} / {}", self.title, self.body);
                record("logged");
                return true;
            }
            (Sender::Apns(a), APNS_PRODUCTION) => (&a.production, &a.topic),
            (Sender::Apns(a), APNS_SANDBOX) => (&a.sandbox, &a.topic),
            _ => {
                record("unknown_service");
                return false;
            }
        };

        use a2::{DefaultNotificationBuilder, NotificationBuilder, NotificationOptions};
        let token = Zeroizing::new(hex::encode(token));
        let mut builder = DefaultNotificationBuilder::new()
            .set_body(&self.body)
            .set_sound("default");
        if !self.title.is_empty() {
            builder = builder.set_title(&self.title);
        }
        let payload = builder.build(
            &token,
            NotificationOptions {
                apns_topic: Some(topic),
                ..Default::default()
            },
        );
        match client.send(payload).await {
            Ok(_) => {
                record("ok");
                true
            }
            Err(e) => {
                // The error can carry the response, never the token itself.
                tracing::debug!("apns: {e}");
                record("apns_error");
                false
            }
        }
    }
}

#[tonic::async_trait]
impl PushService for Arc<Push> {
    async fn notify(&self, req: Request<NotifyRequest>) -> Result<Response<NotifyResponse>, Status> {
        let tokens = req.into_inner().sealed_tokens;
        let sends = tokens.iter().take(MAX_TOKENS).map(|t| self.send_one(t));
        let sent = futures::future::join_all(sends).await.into_iter().filter(|ok| *ok).count();
        Ok(Response::new(NotifyResponse { sent: sent as u32 }))
    }
}

fn apns_sender() -> Result<Sender, BoxError> {
    if std::env::var("APNS").as_deref() == Ok("log") {
        tracing::warn!("APNS=log: alerts are logged, not sent");
        return Ok(Sender::Log);
    }
    let key = secret("apns-key.p8")?;
    let (key_id, team_id) = (env("APNS_KEY_ID")?, env("APNS_TEAM_ID")?);
    let client = |endpoint| {
        a2::Client::token(
            &mut key.as_slice(),
            &key_id,
            &team_id,
            a2::ClientConfig::new(endpoint),
        )
    };
    Ok(Sender::Apns(Box::new(Apns {
        production: client(a2::Endpoint::Production)?,
        sandbox: client(a2::Endpoint::Sandbox)?,
        topic: env("APNS_TOPIC")?,
    })))
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("PUSH_PORT")
        .unwrap_or_else(|_| "9003".into())
        .parse()?;
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("keygen") => {
            let dir = args.get(2).ok_or("usage: zkrp-push keygen <dir>")?;
            return keygen(Path::new(dir));
        }
        Some("healthcheck") => {
            let ok = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                tokio::net::TcpStream::connect(("127.0.0.1", port)),
            )
            .await
            .is_ok_and(|r| r.is_ok());
            std::process::exit(if ok { 0 } else { 1 });
        }
        _ => {}
    }

    telemetry::install("0.0.0.0:9467");
    let key: [u8; 32] = hex::decode(String::from_utf8_lossy(&secret("push-key")?).trim())?
        .try_into()
        .map_err(|_| "push-key isn't 32 bytes (run `zkrp-push keygen` on the secrets directory)")?;
    let push = Arc::new(Push {
        key: Zeroizing::new(key),
        sender: apns_sender()?,
        title: std::env::var("PUSH_ALERT_TITLE").unwrap_or_default(),
        body: std::env::var("PUSH_ALERT_BODY").unwrap_or_else(|_| "New message".into()),
    });

    tracing::info!("zkrp-push {}", telemetry::version());
    tracing::info!("push gRPC on 0.0.0.0:{port}");
    tonic::transport::Server::builder()
        .add_service(PushServiceServer::new(push))
        .serve(format!("0.0.0.0:{port}").parse()?)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push() -> Push {
        let (secret, _) = zkcred::sealed_box::keypair();
        Push {
            key: Zeroizing::new(secret),
            sender: Sender::Log,
            title: String::new(),
            body: "New message".into(),
        }
    }

    #[tokio::test]
    async fn sends_only_what_opens() {
        let p = push();
        let public = zkcred::sealed_box::public_key(&p.key);
        let good = zkcred::sealed_box::seal(&public, &[APNS_SANDBOX, 0xab, 0xcd]);
        let empty = zkcred::sealed_box::seal(&public, &[APNS_SANDBOX]);
        let (_, other) = zkcred::sealed_box::keypair();
        let foreign = zkcred::sealed_box::seal(&other, &[APNS_SANDBOX, 1]);
        let resp = Arc::new(p)
            .notify(Request::new(NotifyRequest { sealed_tokens: vec![good, empty, foreign] }))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().sent, 1);
    }
}
