mod behavior;
mod node;

use behavior::ClientBehavior;
use libghost::{
    identity::NodeIdentity as CoreIdentity, traits::MeshEvent as CoreEvent,
    transport::TransportConfig as CoreConfig,
};
use node::MeshNode as CoreNode;

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum MeshError {
    #[error("Invalid address")]
    InvalidAddress,
    #[error("Connection failed")]
    ConnectionFailed,
    #[error("Timeout")]
    Timeout,
    #[error("Unknown error")]
    Unknown,
}

impl From<Box<dyn std::error::Error>> for MeshError {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        let s = e.to_string();
        if s.contains("invalid multiaddr") || s.contains("parse") {
            MeshError::InvalidAddress
        } else if s.contains("connection") {
            MeshError::ConnectionFailed
        } else {
            MeshError::Unknown
        }
    }
}

// ── NodeIdentity ──────────────────────────────────────────────────────────────

#[derive(uniffi::Object)]
pub struct NodeIdentity {
    inner: CoreIdentity,
}

#[uniffi::export]
impl NodeIdentity {
    #[uniffi::constructor]
    pub fn new() -> Self {
        Self {
            inner: CoreIdentity::generate(),
        }
    }

    pub fn peer_id_string(&self) -> String {
        self.inner.peer_id_string()
    }
}

// ── TransportConfig ───────────────────────────────────────────────────────────

#[derive(uniffi::Object)]
pub struct TransportConfig {
    inner: CoreConfig,
}

#[uniffi::export]
impl TransportConfig {
    #[uniffi::constructor]
    pub fn new(tcp_port: u16, quic_port: u16) -> Self {
        Self {
            inner: CoreConfig::with_ports(tcp_port, quic_port),
        }
    }
}

// ── MeshEvent ─────────────────────────────────────────────────────────────────

#[derive(uniffi::Enum)]
pub enum MeshEvent {
    PeerDiscovered { peer_id: String, addr: String },
    PeerLost { peer_id: String, addr: String },
    RelayReservationAccepted,
    RelayReservationFailed,
}

impl From<CoreEvent> for MeshEvent {
    fn from(e: CoreEvent) -> Self {
        match e {
            CoreEvent::PeerDiscovered { peer_id, addr } => {
                MeshEvent::PeerDiscovered { peer_id, addr }
            }
            CoreEvent::PeerLost { peer_id, addr } => MeshEvent::PeerLost { peer_id, addr },
            CoreEvent::RelayReservationAccepted => MeshEvent::RelayReservationAccepted,
            CoreEvent::RelayReservationFailed => MeshEvent::RelayReservationFailed,
        }
    }
}

// ── MeshNode ──────────────────────────────────────────────────────────────────

#[derive(uniffi::Object)]
pub struct MeshNode {
    inner: std::sync::Mutex<CoreNode>,
}

#[uniffi::export]
impl MeshNode {
    #[uniffi::constructor]
    pub async fn new(
        identity: std::sync::Arc<NodeIdentity>,
        relay_addr: String,
        config: std::sync::Arc<TransportConfig>,
    ) -> Result<Self, MeshError> {
        let keypair_bytes = identity.inner.to_keypair_bytes();
        let core_identity =
            CoreIdentity::from_keypair_bytes(&keypair_bytes).map_err(MeshError::from)?;

        let core_config = CoreConfig::with_ports(config.inner.tcp_port, config.inner.quic_port);

        let node = CoreNode::start(
            core_identity,
            relay_addr,
            core_config,
            |key, relay_client| ClientBehavior::new(key.public(), relay_client, key),
        )
        .await
        .map_err(MeshError::from)?;

        Ok(Self {
            inner: std::sync::Mutex::new(node),
        })
    }

    pub fn drain_events(&self) -> Vec<MeshEvent> {
        self.inner
            .lock()
            .unwrap()
            .drain_events()
            .into_iter()
            .map(MeshEvent::from)
            .collect()
    }
}
