use libghost::behavior::ClientBehavior;
use libghost::context::{ZRPContext, ZRPHandle};
use libghost::handler::{ConnectionStatus, EventHandler, ZRPEvent};
use libghost::identity::NodeIdentity as CoreIdentity;
use libghost::transport::TransportConfig as CoreConfig;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

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
    #[error("Connection error: {msg}")]
    ConnectionError { msg: String },
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

#[uniffi::export(callback_interface)]
pub trait SwiftEventHandler: Send + Sync {
    fn on_message(&self, conversation: String, payload: Vec<u8>, content_type: u16);
    fn on_peer_connected(&self, peer_id: String);
    fn on_peer_disconnected(&self, peer_id: String);
    fn on_connection_status(&self, status: String);
    fn on_send_failed(&self, conversation: String);
}

struct SwiftHandlerBridge {
    inner: Arc<dyn SwiftEventHandler>,
}

impl EventHandler for SwiftHandlerBridge {
    fn handle(&self, event: &ZRPEvent) -> bool {
        match event {
            ZRPEvent::Message {
                conversation,
                payload,
                content_type,
                ..
            } => {
                self.inner
                    .on_message(conversation.clone(), payload.clone(), *content_type);
            }
            ZRPEvent::PeerConnected { peer_id, .. } => {
                self.inner.on_peer_connected(peer_id.to_string());
            }
            ZRPEvent::PeerDisconnected { peer_id, .. } => {
                self.inner.on_peer_disconnected(peer_id.to_string());
            }
            ZRPEvent::ConnectionStatus(status) => {
                let s = match status {
                    ConnectionStatus::Connected { relay } => format!("connected:{}", relay),
                    ConnectionStatus::Disconnected => "disconnected".to_string(),
                    ConnectionStatus::Connecting => "connecting".to_string(),
                    ConnectionStatus::Degraded { reason, .. } => format!("degraded:{}", reason),
                };
                self.inner.on_connection_status(s);
            }
            ZRPEvent::MessageSendFailed { conversation, .. } => {
                self.inner.on_send_failed(conversation.clone());
            }
            _ => {}
        }
        true
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

impl Default for NodeIdentity {
    fn default() -> Self {
        Self {
            inner: CoreIdentity::generate(),
        }
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

#[derive(uniffi::Object)]
pub struct MeshNode {
    handle: Mutex<Option<ZRPHandle>>,
}

#[uniffi::export]
impl MeshNode {
    #[uniffi::constructor]
    pub async fn new(
        identity: Arc<NodeIdentity>,
        relay_addr: String,
        config: Arc<TransportConfig>,
        handler: Box<dyn SwiftEventHandler>,
    ) -> Result<Self, MeshError> {
        let core_identity = CoreIdentity::from_keypair_bytes(&identity.inner.to_keypair_bytes())
            .map_err(MeshError::from)?;

        let core_config = CoreConfig::with_ports(config.inner.tcp_port, config.inner.quic_port);

        let bridge = SwiftHandlerBridge {
            inner: Arc::from(handler),
        };

        let handle = get_runtime()
            .spawn(async move {
                let mut ctx = ZRPContext::default();
                ctx.register_handler("swift", bridge).await;
                ctx.start(
                    core_identity,
                    None,
                    None,
                    core_config,
                    |key, relay_client| ClientBehavior::new(key.public(), relay_client, key),
                )
                .await
                .map_err(|e| e.to_string())
            })
            .await
            .map_err(|_| MeshError::Unknown)?
            .map_err(|e| MeshError::ConnectionError { msg: e })?;

        Ok(Self {
            handle: Mutex::new(Some(handle)),
        })
    }

    pub async fn subscribe(&self, topic: String) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.subscribe(topic).await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn send_message(&self, topic: String, text: String) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.publish(topic, text.into_bytes()).await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn unsubscribe(&self, topic: String) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.unsubscribe(topic).await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn shutdown(&self) {
        if let Some(h) = self.handle.lock().await.take() {
            h.shutdown().await;
        }
    }
}
