use libghost::behavior::ClientBehavior;
use libghost::node::MeshNode as CoreNode;
use libghost::{
    identity::NodeIdentity as CoreIdentity, traits::MeshEvent as CoreEvent,
    transport::TransportConfig as CoreConfig,
};
use std::sync::OnceLock;

static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn get_runtime() -> &'static tokio::runtime::Runtime {
    TOKIO_RUNTIME
        .get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"))
}
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
            CoreEvent::RelayReservationAccepted { .. } => MeshEvent::RelayReservationAccepted,
            CoreEvent::RelayReservationFailed => MeshEvent::RelayReservationFailed,
        }
    }
}

#[derive(uniffi::Object)]
pub struct MeshNode {
    inner: tokio::sync::Mutex<CoreNode>,
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

        let node = get_runtime()
            .spawn(async move {
                CoreNode::start(
                    core_identity,
                    relay_addr,
                    core_config,
                    |key, relay_client| ClientBehavior::new(key.public(), relay_client, key),
                )
                .await
                .map_err(|e| e.to_string())
            })
            .await
            .map_err(|e| MeshError::Unknown)?
            .map_err(|e| MeshError::Unknown)?;

        Ok(Self {
            inner: tokio::sync::Mutex::new(node),
        })
    }

    pub fn drain_events(&self) -> Vec<MeshEvent> {
        self.inner
            .blocking_lock()
            .drain_events()
            .into_iter()
            .map(MeshEvent::from)
            .collect()
    }

    pub async fn send_message(&self, topic: String, text: String) -> Result<(), MeshError> {
        self.inner
            .blocking_lock()
            .send_message(&topic, text.into_bytes())
            .await
            .map_err(MeshError::from)
    }

    pub fn drain_messages(&self) -> Vec<GhostMessage> {
        self.inner
            .blocking_lock()
            .drain_messages()
            .into_iter()
            .map(|env| GhostMessage {
                payload: String::from_utf8_lossy(&env.payload).to_string(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            })
            .collect()
    }

    pub async fn subscribe(&self, topic: String) -> Result<(), MeshError> {
        self.inner
            .lock()
            .await
            .subscribe(&topic)
            .await
            .map_err(MeshError::from)
    }
}

#[derive(uniffi::Record)]
pub struct GhostMessage {
    pub payload: String,
    pub timestamp: u64,
}
