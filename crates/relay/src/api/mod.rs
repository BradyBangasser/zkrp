use std::future::Future;
use std::process::ExitCode;
use std::time::Duration;

use tonic::transport::Server;

use crate::{
    BoxError, RelayState,
    api::{
        blob::BlobService,
        log::LogService,
        mailbox::{MailboxServiceImpl, Pusher, proto::mailbox_service_server::MailboxServiceServer},
        proto::{
            blob_store_server::BlobStoreServer, logging_server::LoggingServer,
            relay_service_client::RelayServiceClient, relay_service_server::RelayServiceServer,
        },
        relay::RelayServiceImpl,
    },
    admission::Admission,
    mailbox_store::MailboxStore,
};
use libghost::admit::{SERVICE_BLOB, SERVICE_LOGGING, SERVICE_MAILBOX};

mod blob;
mod log;
mod mailbox;
pub(crate) mod relay;

const SERVICES: &[&str] = &[
    "zrp.relay.v1.RelayService",
    SERVICE_BLOB,
    SERVICE_LOGGING,
    SERVICE_MAILBOX,
];

pub mod proto {
    tonic::include_proto!("zrp.relay.v1");
}

/// Serve every gRPC service until `shutdown` resolves. Constructing the
/// services does no I/O, so a storage outage can't stop the API from coming up;
/// S3 failures surface per request instead.
pub async fn serve(
    port: u16,
    state: RelayState,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), BoxError> {
    // Behind a TLS proxy (Caddy) set GRPC_BIND=127.0.0.1 so plaintext gRPC is
    // never reachable from outside the host.
    let bind = std::env::var("GRPC_BIND").unwrap_or_else(|_| "0.0.0.0".to_string());
    let addr = format!("{bind}:{port}").parse()?;

    let aws = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&aws);

    Server::builder()
        .timeout(Duration::from_secs(60))
        .layer(telemetry::GrpcMetricsLayer::new(SERVICES))
        // Services that cost the relay storage require an admission; discovery
        // (RelayService) stays open so a client can find relays before it
        // has a credential.
        .add_service(BlobStoreServer::with_interceptor(
            BlobService::new(s3.clone(), state.config.clone()),
            Admission::interceptor(state.admission.clone(), SERVICE_BLOB),
        ))
        .add_service(LoggingServer::with_interceptor(
            LogService::new(s3.clone(), state.config.clone()),
            Admission::interceptor(state.admission.clone(), SERVICE_LOGGING),
        ))
        .add_service(MailboxServiceServer::with_interceptor(
            MailboxServiceImpl {
                store: MailboxStore::new(s3, state.config.mailbox_bucket.clone()),
                push: state.config.push.as_ref().map(Pusher::new),
            },
            Admission::interceptor(state.admission.clone(), SERVICE_MAILBOX),
        ))
        .add_service(RelayServiceServer::new(RelayServiceImpl { state }))
        .serve_with_shutdown(addr, shutdown)
        .await?;

    Ok(())
}

/// Container HEALTHCHECK: ask the local gRPC server for Health. Exits 0 when the
/// relay answers HEALTHY or DEGRADED (it is up and should not be restarted),
/// 1 otherwise.
pub fn healthcheck() -> ExitCode {
    let port = std::env::var("GRPC_PORT").unwrap_or_else(|_| "9001".into());
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(_) => return ExitCode::FAILURE,
    };
    let ok = rt.block_on(async {
        let call = async {
            let mut client = RelayServiceClient::connect(format!("http://127.0.0.1:{port}")).await?;
            let status = client.health(()).await?.into_inner().status;
            Ok::<_, BoxError>(status != proto::HealthStatus::Unhealthy as i32)
        };
        matches!(tokio::time::timeout(Duration::from_secs(3), call).await, Ok(Ok(true)))
    });
    if ok { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}

/// Plain-HTTP `GET /healthz` for DNS load-balancer health monitors (Cloudflare,
/// Route 53, uptime checkers), which can't speak gRPC. 200 when this relay can
/// serve clients, 503 when it has no public address (it would accept
/// connections but grant no reservations). Caddy exposes it over TLS.
pub async fn serve_healthz(state: RelayState) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = std::env::var("HEALTH_ADDR").unwrap_or_else(|_| "0.0.0.0:9466".to_string());
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("healthz on {addr}: {e}; disabled");
            return;
        }
    };
    tracing::info!("healthz on http://{addr}/healthz");
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        let healthy = state.has_external_addr;
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let Ok(Ok(n)) =
                tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf)).await
            else {
                return;
            };
            let req = &buf[..n];
            let (status, body) = if !(req.starts_with(b"GET /healthz ")
                || req.starts_with(b"HEAD /healthz "))
            {
                ("404 Not Found", "")
            } else if healthy {
                ("200 OK", "ok\n")
            } else {
                ("503 Service Unavailable", "no public address\n")
            };
            let head = req.starts_with(b"HEAD ");
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{}",
                body.len(),
                if head { "" } else { body }
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}
