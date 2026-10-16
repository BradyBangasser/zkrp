mod attest;
mod mailer;
mod nullifier;
mod service;
mod token;

pub mod proto {
    tonic::include_proto!("zrp.auth.v1");
}

use std::path::Path;
use std::sync::Arc;
use tonic::transport::Server;

use crate::attest::{AppAttest, Environment};
use crate::mailer::{LogMailer, Mailer, SesMailer};
use crate::nullifier::{KmsPepper, LocalPepper, NullifierSet, Pepper};
use crate::proto::auth_service_server::AuthServiceServer;
use crate::service::{Auth, SendLimiter};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Secrets never live in the image or the environment. systemd
/// `LoadCredential=` or compose secrets put them in a tmpfs at
/// CREDENTIALS_DIRECTORY, hex-encoded.
fn secret(name: &str) -> Result<Vec<u8>, BoxError> {
    let dir = std::env::var("CREDENTIALS_DIRECTORY")
        .map_err(|_| format!("CREDENTIALS_DIRECTORY unset; refusing to invent {name}"))?;
    let raw = std::fs::read_to_string(Path::new(&dir).join(name))
        .map_err(|e| format!("credential {name}: {e}"))?;
    Ok(hex::decode(raw.trim()).map_err(|e| format!("credential {name} not hex: {e}"))?)
}

fn env(k: &str) -> Result<String, BoxError> {
    std::env::var(k).map_err(|_| format!("{k} not set").into())
}

fn params_label() -> String {
    std::env::var("PARAMS_LABEL").unwrap_or_else(|_| zkcred::DEFAULT_PARAMS_LABEL.into())
}

/// `zkrp-auth keygen <dir>`: write fresh hex secrets (token-key, issuer-key,
/// nullifier-key) with mode 0600, plus issuer-public-key for the relays.
fn keygen(dir: &Path) -> Result<(), BoxError> {
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::create_dir_all(dir)?;
    let write = |name: &str, bytes: &[u8], private: bool| -> Result<(), BoxError> {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        opts.mode(if private { 0o600 } else { 0o644 });
        let mut f = opts
            .open(dir.join(name))
            .map_err(|e| format!("{}: {e}", dir.join(name).display()))?;
        writeln!(f, "{}", hex::encode(bytes))?;
        Ok(())
    };
    let random32 = || {
        let mut k = [0u8; 32];
        getrandom::fill(&mut k).expect("OS RNG");
        k
    };

    let params = zkcred::Params::new(&params_label());
    let issuer = zkcred::IssuerSecretKey::generate(&mut zkcred::rand::rngs::OsRng);
    let public = issuer.public_key(&params).to_bytes();

    write("token-key", &random32(), true)?;
    write("nullifier-key", &random32(), true)?;
    write("issuer-key", &issuer.to_bytes(), true)?;
    write("issuer-public-key", &public, false)?;
    println!("ISSUER_PUBLIC_KEY={}", hex::encode(&public));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let _ = dotenv::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    // Container HEALTHCHECK: is the gRPC port accepting connections?
    if args.get(1).map(String::as_str) == Some("healthcheck") {
        let port = std::env::var("AUTH_PORT").unwrap_or_else(|_| "9002".into());
        let ok = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")),
        )
        .await
        .is_ok_and(|r| r.is_ok());
        std::process::exit(if ok { 0 } else { 1 });
    }
    if args.get(1).map(String::as_str) == Some("keygen") {
        let dir = args.get(2).ok_or("usage: zkrp-auth keygen <dir>")?;
        return keygen(Path::new(dir));
    }

    telemetry::install("0.0.0.0:9465");

    let port: u16 = std::env::var("AUTH_PORT")
        .unwrap_or_else(|_| "9002".into())
        .parse()?;

    let allowed_domains: Vec<String> = env("ALLOWED_EMAIL_DOMAINS")?
        .split(',')
        .map(|s| s.trim().trim_start_matches('.').to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    if allowed_domains.is_empty() {
        return Err("ALLOWED_EMAIL_DOMAINS is empty".into());
    }

    let aws = aws_config::load_from_env().await;

    let link_base = env("VERIFY_LINK_BASE")?;
    let mailer: Arc<dyn Mailer> = match std::env::var("MAILER").as_deref() {
        Ok("log") => Arc::new(LogMailer { link_base }),
        _ => Arc::new(SesMailer::new(
            aws_sdk_sesv2::Client::new(&aws),
            env("MAIL_FROM")?,
            link_base,
        )),
    };

    // A debug build on a developer's laptop must not enroll against
    // production, and a release build can never skip attestation.
    let attest_env = match std::env::var("APP_ATTEST_ENV").as_deref() {
        Ok("development") => Environment::Development,
        Ok("disabled") if cfg!(debug_assertions) => Environment::Disabled,
        Ok("disabled") => return Err("APP_ATTEST_ENV=disabled requires a debug build".into()),
        _ => Environment::Production,
    };
    let attest = AppAttest::new(&env("APP_ATTEST_APP_ID")?, attest_env);

    let pepper: Arc<dyn Pepper> = match std::env::var("NULLIFIER_KMS_KEY_ID") {
        Ok(key_id) if !key_id.is_empty() => Arc::new(KmsPepper {
            client: aws_sdk_kms::Client::new(&aws),
            key_id,
        }),
        _ => {
            tracing::warn!("NULLIFIER_KMS_KEY_ID unset; using local nullifier-key credential");
            Arc::new(LocalPepper(ring::hmac::Key::new(
                ring::hmac::HMAC_SHA256,
                &secret("nullifier-key")?,
            )))
        }
    };

    // Fails closed: if the DB is unreachable we refuse to start rather than
    // enroll students we cannot record.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&env("DATABASE_URL")?)
        .await?;

    let params = zkcred::Params::new(&params_label());
    let issuer = zkcred::IssuerSecretKey::from_bytes(&secret("issuer-key")?)?;
    let issuer_public = issuer.public_key(&params).to_bytes();
    let epoch_secs: u64 = std::env::var("EPOCH_SECS")
        .unwrap_or_else(|_| "2592000".into())
        .parse()?;

    let auth = Auth {
        mailer,
        allowed_domains,
        token_key: token::TokenKey::new(&secret("token-key")?)?,
        attest,
        nullifiers: NullifierSet { pool, pepper },
        params,
        issuer: Arc::new(issuer),
        issuer_public: issuer_public.clone(),
        epoch_secs,
        limiter: SendLimiter::new(),
    };

    tracing::info!("zkrp-auth {}", telemetry::version());
    tracing::info!(
        "auth gRPC on 0.0.0.0:{port} (app_attest={attest_env:?}, epoch_secs={epoch_secs}, issuer_public_key={})",
        hex::encode(&issuer_public)
    );

    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };

    Server::builder()
        .layer(telemetry::GrpcMetricsLayer::new(&["zrp.auth.v1.AuthService"]))
        .add_service(AuthServiceServer::new(auth))
        .serve_with_shutdown(
            format!("{}:{port}", std::env::var("AUTH_BIND").unwrap_or_else(|_| "0.0.0.0".into()))
                .parse()?,
            shutdown,
        )
        .await?;
    Ok(())
}
