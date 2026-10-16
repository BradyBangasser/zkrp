//! One way to open a gRPC channel to zkrp infrastructure.
//!
//! `https://` addresses get TLS with the bundled Mozilla root store (no
//! platform trust store needed, so it behaves the same on iOS and Linux).
//! `http://` stays plaintext for local development only: in production, mailbox
//! ids, admissions, and email addresses must never cross the network in clear.

use std::time::Duration;

use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Generous enough for a 10 MB photo upload on a slow cellular link.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

pub fn endpoint(addr: &str) -> Result<Endpoint, tonic::transport::Error> {
    let mut ep = Endpoint::from_shared(addr.to_string())?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_while_idle(false);
    if addr.starts_with("https://") {
        ep = ep.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
    } else {
        tracing::warn!("plaintext gRPC to {addr}; use https:// outside local development");
    }
    Ok(ep)
}

pub async fn connect(addr: &str) -> Result<Channel, tonic::transport::Error> {
    endpoint(addr)?.connect().await
}
