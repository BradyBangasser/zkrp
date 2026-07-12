mod mailer;
mod service;

pub mod proto {
    tonic::include_proto!("zrp.auth.v1");
}

use std::sync::Arc;
use tonic::transport::Server;
use tracing::Level;

use crate::mailer::SesMailer;
use crate::proto::auth_service_server::AuthServiceServer;
use crate::service::Auth;

/// Secrets never live on disk in the image. systemd `LoadCredential=` puts them
/// in a tmpfs at CREDENTIALS_DIRECTORY. The KMS pepper is never read at all.
fn secret(name: &str) -> Vec<u8> {
    let dir = std::env::var("CREDENTIALS_DIRECTORY")
        .unwrap_or_else(|_| panic!("CREDENTIALS_DIRECTORY unset; refusing to invent {name}"));
    let raw = std::fs::read_to_string(std::path::Path::new(&dir).join(name))
        .unwrap_or_else(|e| panic!("credential {name}: {e}"));
    hex::decode(raw.trim()).unwrap_or_else(|e| panic!("credential {name} not hex: {e}"))
}

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("{k} not set"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenv::dotenv();
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let port: u16 = std::env::var("AUTH_PORT")
        .unwrap_or_else(|_| "9002".into())
        .parse()?;

    let allowed_domains: Vec<String> = env("ALLOWED_EMAIL_DOMAINS")
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    assert!(!allowed_domains.is_empty(), "no eligible email domains");

    let aws = aws_config::load_from_env().await;

    let mailer = Arc::new(SesMailer::new(
        aws_sdk_sesv2::Client::new(&aws),
        env("MAIL_FROM"),
        env("VERIFY_LINK_BASE"),
    ));

    // Fails closed: if the DB is unreachable we refuse to start rather than
    // enroll students we cannot record.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&env("DATABASE_URL"))
        .await?;

    // In production, a debug build on a developer's laptop must not enroll.
    let production = std::env::var("APP_ATTEST_ENV").as_deref() != Ok("development");

    let auth = Auth {
        mailer,
        allowed_domains,
    };

    tracing::info!("auth gRPC on 0.0.0.0:{port} (app_attest_production={production})");
    Server::builder()
        .add_service(AuthServiceServer::new(auth))
        .serve(format!("0.0.0.0:{port}").parse()?)
        .await?;
    Ok(())
}
