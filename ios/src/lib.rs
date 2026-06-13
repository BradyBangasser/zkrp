use libghost::behavior::ClientBehavior;
use libghost::blob::BlobError;
use libghost::context::{SwarmCommand, ZRPContext, ZRPHandle};
use libghost::handler::{ConnectionStatus, EventHandler, ZRPEvent};
use libghost::identity::NodeIdentity as CoreIdentity;
use libghost::store::{GhostStore, SqliteStore};
use libghost::transport::TransportConfig as CoreConfig;
use std::sync::Once;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
pub const CONTENT_TYPE_PROFILE: u16 = 0x0010;

static INIT_LOGGING: Once = Once::new();

fn init_logging() {
    INIT_LOGGING.call_once(|| {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
    });
}

fn get_runtime() -> &'static tokio::runtime::Runtime {
    TOKIO_RUNTIME
        .get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"))
}

fn documents_dir() -> String {
    std::env::var("HOME")
        .map(|h| {
            if cfg!(target_os = "ios") {
                format!("{}/Documents", h)
            } else {
                format!("{}/.ghost", h)
            }
        })
        .unwrap_or_else(|_| "/tmp".to_string())
}

fn ghost_db_path() -> String {
    format!("{}/ghost.db", documents_dir())
}

fn make_store() -> Arc<dyn GhostStore> {
    Arc::new(SqliteStore::new(&ghost_db_path()))
}

uniffi::setup_scaffolding!();

// MARK: - Error

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
    #[error("Blob error: {msg}")]
    BlobError { msg: String },
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

impl From<BlobError> for MeshError {
    fn from(e: BlobError) -> Self {
        match e {
            BlobError::Timeout => MeshError::Timeout,
            BlobError::NotFound => MeshError::ConnectionError {
                msg: "Blob not found".into(),
            },
            other => MeshError::BlobError {
                msg: other.to_string(),
            },
        }
    }
}

#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct PeerProfile {
    pub peer_id: String,
    pub name: String,
    pub college: String,
    pub year: String,
    pub bio: String,
    pub photo_blob_ids: Vec<String>,
    pub timestamp: u64,
}

// MARK: - SwiftEventHandler

#[uniffi::export(callback_interface)]
pub trait SwiftEventHandler: Send + Sync {
    fn on_message(
        &self,
        peer_id: String,
        conversation: String,
        payload: Vec<u8>,
        content_type: u16,
    );
    fn on_peer_connected(&self, peer_id: String);
    fn on_peer_disconnected(&self, peer_id: String);
    fn on_connection_status(&self, status: String);
    fn on_send_failed(&self, conversation: String);
    fn on_profile_received(&self, profile: PeerProfile);
}

#[derive(Clone)]
struct SwiftHandlerBridge {
    inner: Arc<dyn SwiftEventHandler>,
    // handle: Option<Arc<std::sync::Mutex<ZRPHandle>>>,
    profile: Arc<std::sync::Mutex<PeerProfile>>,
}

impl EventHandler for SwiftHandlerBridge {
    fn handle(&self, event: &ZRPEvent, tx: tokio::sync::mpsc::Sender<SwarmCommand>) -> bool {
        match event {
            ZRPEvent::Message {
                conversation,
                payload,
                content_type,
                peer_id,
                ..
            } => match content_type {
                0x0010 => {
                    if let Ok(profile) = postcard::from_bytes::<PeerProfile>(payload) {
                        self.inner.on_profile_received(profile.clone());

                        let our_profile = self.profile.lock().unwrap().clone();
                        let reply_topic = format!("fratrat/v1/profiles/dm/{}", peer_id);
                        let payload = postcard::to_allocvec(&our_profile).unwrap_or_default();
                        tokio::spawn(async move {
                            ZRPHandle::send(tx, reply_topic, payload).await;
                        });
                    }
                }
                _ => {
                    self.inner.on_message(
                        peer_id.to_string(),
                        conversation.clone(),
                        payload.clone(),
                        *content_type,
                    );
                }
            },
            ZRPEvent::PeerConnected { peer_id, .. } => {
                self.inner.on_peer_connected(peer_id.to_string());

                let our_profile = self.profile.lock().unwrap().clone();
                let payload = postcard::to_allocvec(&our_profile).unwrap_or_default();
                tokio::spawn(async move {
                    ZRPHandle::send(tx, "fratrat/v1/profiles".to_string(), payload).await;
                });
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

// MARK: - NodeIdentity

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

    #[uniffi::constructor]
    pub fn load_or_generate(db_path: String) -> Self {
        let store: Arc<dyn GhostStore> = Arc::new(SqliteStore::new(&db_path));
        Self {
            inner: CoreIdentity::load_or_generate(&store),
        }
    }

    pub fn peer_id_string(&self) -> String {
        self.inner.peer_id_string()
    }
}

impl Default for NodeIdentity {
    fn default() -> Self {
        Self {
            inner: CoreIdentity::load_or_generate(&make_store()),
        }
    }
}

// MARK: - TransportConfig

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

// MARK: - MeshNode

#[derive(uniffi::Object)]
pub struct MeshNode {
    handle: Mutex<Option<ZRPHandle>>,
}

#[uniffi::export]
#[allow(unused)]
impl MeshNode {
    #[uniffi::constructor]
    pub async fn new(
        identity: Arc<NodeIdentity>,
        relay_addr: String,
        config: Arc<TransportConfig>,
        handler: Box<dyn SwiftEventHandler>,
        profile: PeerProfile,
    ) -> Result<Self, MeshError> {
        init_logging();
        let core_identity = CoreIdentity::from_keypair_bytes(&identity.inner.to_keypair_bytes())
            .map_err(MeshError::from)?;

        let core_config = CoreConfig::with_ports(config.inner.tcp_port, config.inner.quic_port);

        let mut bridge = SwiftHandlerBridge {
            inner: Arc::from(handler),
            profile: Arc::new(std::sync::Mutex::new(profile)),
        };

        let db_path = ghost_db_path();

        let handle = get_runtime()
            .spawn(async move {
                let store = make_store();
                let mut ctx = ZRPContext::with_store(Arc::clone(&store));
                ctx.register_handler("swift", bridge).await;
                ctx.start(
                    core_identity,
                    Some(vec![relay_addr]),
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

        let my_peer_id = identity.peer_id_string();
        handle
            .subscribe(format!("fratrat/v1/profiles/dm/{}", my_peer_id))
            .await;
        handle.subscribe("fratrat/v1/profiles".to_string()).await;

        Ok(Self {
            handle: Mutex::new(Some(handle)),
        })
    }

    // MARK: - Messaging

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

    pub async fn send_raw(&self, topic: String, payload: Vec<u8>) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.publish(topic, payload).await;
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

    // MARK: - Blob storage

    /// Upload bytes to the mesh. Returns blob_id as hex string.
    /// Share this ID with recipients — they use it to retrieve.
    pub async fn upload_blob(&self, data: Vec<u8>) -> Result<String, MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.upload_blob(data).await.map_err(MeshError::from)
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    /// Retrieve a blob by hex ID. Collects chunks from mesh peers.
    /// Returns raw bytes — caller decodes (e.g. as JPEG).
    pub async fn retrieve_blob(&self, blob_id: String) -> Result<Vec<u8>, MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.retrieve_blob(&blob_id).await.map_err(MeshError::from)
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    /// Bytes currently used by blob chunk storage on this device
    pub async fn blob_storage_bytes(&self) -> u64 {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.blob_storage_bytes()
        } else {
            0
        }
    }

    /// Evict chunks for blobs older than max_age_secs.
    /// Call on app background / low storage warnings.
    pub async fn evict_old_blobs(&self, max_age_secs: u64) {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.evict_old_blobs(max_age_secs);
        }
    }

    // MARK: - Lifecycle

    pub async fn shutdown(&self) {
        if let Some(h) = self.handle.lock().await.take() {
            h.shutdown().await;
        }
    }
}

// MARK: - Relay discovery

#[uniffi::export]
pub fn discover_relays(grpc_addr: String) -> Vec<RelayInfo> {
    get_runtime().block_on(async move {
        match libghost::relay::RelayClient::connect(&grpc_addr).await {
            Ok(mut client) => client
                .list_relays()
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|r| RelayInfo {
                    multiaddr: r.multiaddr,
                    peer_id: r.peer_id,
                    region: r.region,
                    load: r.load,
                    meshes: r.meshes,
                })
                .collect(),
            Err(_) => vec![],
        }
    })
}

// MARK: - Records

#[derive(uniffi::Record)]
pub struct RelayInfo {
    pub multiaddr: String,
    pub peer_id: String,
    pub region: String,
    pub load: u32,
    pub meshes: Vec<String>,
}
