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
pub const CONTENT_TYPE_PROFILE_REPLY: u16 = 0x0011;
pub const CONTENT_TYPE_PROFILE_BACKFILL: u16 = 0x0012;

pub const CONTENT_TYPE_PARTY: u16 = 0x0020;
pub const CONTENT_TYPE_PARTY_REPLY: u16 = 0x0021;
pub const CONTENT_TYPE_PARTY_BACKFILL: u16 = 0x0022;

pub const CONTENT_TYPE_ALERT: u16 = 0x0030;
pub const CONTENT_TYPE_ALERT_REPLY: u16 = 0x0031;
pub const CONTENT_TYPE_ALERT_BACKFILL: u16 = 0x0032;

const PROFILE_STORE_KEY: &str = "fratrat/profile";
const PEER_PROFILE_PREFIX: &str = "fratrat/peer_profile/";
const PEER_LAST_SEEN_PREFIX: &str = "fratrat/peer_last_seen/";

const PARTY_PREFIX: &str = "fratrat/party/";
const ALERT_PREFIX: &str = "fratrat/alert/";

static INIT_LOGGING: Once = Once::new();

fn init_logging() {
    INIT_LOGGING.call_once(|| {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
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

static SHARED_STORE: OnceLock<Arc<dyn GhostStore>> = OnceLock::new();

fn store() -> Arc<dyn GhostStore> {
    Arc::clone(SHARED_STORE.get_or_init(|| Arc::new(SqliteStore::new(&ghost_db_path()))))
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct PartyProfile {
    pub id: u64,
    pub title: String,
    pub vibe: String,
    pub cap: u16,
    pub attend: u16,
    pub expires: u64,
    pub host: String,
    pub address: String,
    pub lat: f64,
    pub lon: f64,
    pub description: String,
    pub img_url: Option<String>,
    pub timestamp: u64,
}

#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct AlertProfile {
    pub name: String,
    pub id: u64,
    pub description: String,
    pub expires: u64,
    pub lat: f64,
    pub lon: f64,
    pub timestamp: u64,
}

fn peer_profile_key(peer_id: &str) -> String {
    format!("{}{}", PEER_PROFILE_PREFIX, peer_id)
}

fn peer_last_seen_key(peer_id: &str) -> String {
    format!("{}{}", PEER_LAST_SEEN_PREFIX, peer_id)
}

fn party_key(id: u64) -> String {
    format!("{}{}", PARTY_PREFIX, id)
}

fn alert_key(id: u64) -> String {
    format!("{}{}", ALERT_PREFIX, id)
}

fn save_if_newer<T: serde::Serialize + serde::de::DeserializeOwned>(
    store: &Arc<dyn GhostStore>,
    key: &str,
    item: &T,
    item_ts: u64,
    existing_ts_fn: impl Fn(&T) -> u64,
) -> bool {
    let existing_ts = store
        .get(key)
        .and_then(|b| postcard::from_bytes::<T>(&b).ok())
        .map(|existing| existing_ts_fn(&existing))
        .unwrap_or(0);
    if item_ts > existing_ts
        && let Ok(bytes) = postcard::to_allocvec(item)
    {
        store.set(key, bytes);
        return true;
    }
    false
}

#[uniffi::export]
pub fn save_user_profile(profile: PeerProfile) -> bool {
    let store = store();
    match postcard::to_allocvec(&profile) {
        Ok(bytes) => {
            store.set(PROFILE_STORE_KEY, bytes);
            tracing::info!("save_user_profile: saved for {}", profile.peer_id);
            true
        }
        Err(e) => {
            tracing::error!("save_user_profile: serialize error: {}", e);
            false
        }
    }
}

#[uniffi::export]
pub fn load_user_profile() -> Option<PeerProfile> {
    let store = store();
    let bytes = store.get(PROFILE_STORE_KEY)?;
    match postcard::from_bytes::<PeerProfile>(&bytes) {
        Ok(p) => {
            tracing::info!("load_user_profile: loaded for {}", p.peer_id);
            Some(p)
        }
        Err(e) => {
            tracing::error!("load_user_profile: deserialize error: {}", e);
            None
        }
    }
}

#[uniffi::export]
pub fn save_peer_profile(profile: PeerProfile) -> bool {
    let store = store();
    match postcard::to_allocvec(&profile) {
        Ok(bytes) => {
            store.set(&peer_profile_key(&profile.peer_id), bytes);
            true
        }
        Err(e) => {
            tracing::error!("save_peer_profile: serialize error: {}", e);
            false
        }
    }
}

#[uniffi::export]
pub fn load_peer_profile(peer_id: String) -> Option<PeerProfile> {
    let store = store();
    let bytes = store.get(&peer_profile_key(&peer_id))?;
    postcard::from_bytes::<PeerProfile>(&bytes).ok()
}

#[uniffi::export]
pub fn load_all_peer_profiles() -> Vec<PeerProfile> {
    let store = store();
    store
        .list(PEER_PROFILE_PREFIX)
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<PeerProfile>(&bytes).ok())
        .collect()
}

#[uniffi::export]
pub fn load_peer_profiles_since(since_unix_secs: u64) -> Vec<PeerProfile> {
    let store = store();
    store
        .list(PEER_PROFILE_PREFIX)
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<PeerProfile>(&bytes).ok())
        .filter(|p| p.timestamp > since_unix_secs)
        .collect()
}

#[uniffi::export]
pub fn update_peer_last_seen(peer_id: String) {
    let store = store();
    let ts = now_unix().to_le_bytes().to_vec();
    store.set(&peer_last_seen_key(&peer_id), ts);
}

#[uniffi::export]
pub fn get_peer_last_seen(peer_id: String) -> u64 {
    let store = store();
    store
        .get(&peer_last_seen_key(&peer_id))
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

#[uniffi::export]
pub fn save_party(party: PartyProfile) -> bool {
    let store = store();
    match postcard::to_allocvec(&party) {
        Ok(bytes) => {
            store.set(&party_key(party.id), bytes);
            true
        }
        Err(e) => {
            tracing::error!("save_party: {}", e);
            false
        }
    }
}

#[uniffi::export]
pub fn load_party(id: u64) -> Option<PartyProfile> {
    let store = store();
    let bytes = store.get(&party_key(id))?;
    postcard::from_bytes::<PartyProfile>(&bytes).ok()
}

#[uniffi::export]
pub fn load_all_parties() -> Vec<PartyProfile> {
    let store = store();
    store
        .list(PARTY_PREFIX)
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<PartyProfile>(&bytes).ok())
        .collect()
}

#[uniffi::export]
pub fn load_parties_since(since_unix_secs: u64) -> Vec<PartyProfile> {
    let store = store();
    store
        .list(PARTY_PREFIX)
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<PartyProfile>(&bytes).ok())
        .filter(|p| p.timestamp > since_unix_secs)
        .collect()
}

#[uniffi::export]
pub fn delete_party(id: u64) {
    store().delete(&party_key(id));
}

#[uniffi::export]
pub fn save_alert(alert: AlertProfile) -> bool {
    let store = store();
    match postcard::to_allocvec(&alert) {
        Ok(bytes) => {
            store.set(&alert_key(alert.id), bytes);
            true
        }
        Err(e) => {
            tracing::error!("save_alert: {}", e);
            false
        }
    }
}

#[uniffi::export]
pub fn load_alert(id: u64) -> Option<AlertProfile> {
    let store = store();
    let bytes = store.get(&alert_key(id))?;
    postcard::from_bytes::<AlertProfile>(&bytes).ok()
}

#[uniffi::export]
pub fn load_all_alerts() -> Vec<AlertProfile> {
    let store = store();
    store
        .list(ALERT_PREFIX)
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<AlertProfile>(&bytes).ok())
        .collect()
}

#[uniffi::export]
pub fn load_alerts_since(since_unix_secs: u64) -> Vec<AlertProfile> {
    let store = store();
    store
        .list(ALERT_PREFIX)
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<AlertProfile>(&bytes).ok())
        .filter(|a| a.timestamp > since_unix_secs)
        .collect()
}

#[uniffi::export]
pub fn delete_alert(id: u64) {
    store().delete(&alert_key(id));
}

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
    fn on_profiles_backfilled(&self, profiles: Vec<PeerProfile>);

    fn on_party_received(&self, party: PartyProfile);
    fn on_parties_backfilled(&self, parties: Vec<PartyProfile>);

    fn on_alert_received(&self, alert: AlertProfile);
    fn on_alerts_backfilled(&self, alerts: Vec<AlertProfile>);
}

#[derive(Clone)]
struct SwiftHandlerBridge {
    inner: Arc<dyn SwiftEventHandler>,
    profile: Arc<std::sync::Mutex<PeerProfile>>,
    store: Arc<dyn GhostStore>,
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
            } => match *content_type {
                CONTENT_TYPE_PROFILE => {
                    if let Ok(profile) = postcard::from_bytes::<PeerProfile>(payload) {
                        if let Ok(bytes) = postcard::to_allocvec(&profile) {
                            self.store.set(&peer_profile_key(&profile.peer_id), bytes);
                        }
                        self.inner.on_profile_received(profile.clone());

                        let our_profile = self.profile.lock().unwrap().clone();
                        let reply_payload = postcard::to_allocvec(&our_profile).unwrap_or_default();
                        let reply_topic = format!("fratrat/v1/profiles/dm/{}", peer_id);
                        tokio::spawn(async move {
                            ZRPHandle::send_typed(
                                tx,
                                reply_topic,
                                reply_payload,
                                CONTENT_TYPE_PROFILE_REPLY,
                            )
                            .await;
                        });
                    }
                }

                CONTENT_TYPE_PROFILE_REPLY => {
                    if let Ok(profile) = postcard::from_bytes::<PeerProfile>(payload) {
                        if let Ok(bytes) = postcard::to_allocvec(&profile) {
                            self.store.set(&peer_profile_key(&profile.peer_id), bytes);
                        }
                        self.inner.on_profile_received(profile);
                    }
                }

                CONTENT_TYPE_PROFILE_BACKFILL => {
                    if let Ok(profiles) = postcard::from_bytes::<Vec<PeerProfile>>(payload) {
                        for profile in &profiles {
                            let existing_ts = self
                                .store
                                .get(&peer_profile_key(&profile.peer_id))
                                .and_then(|b| postcard::from_bytes::<PeerProfile>(&b).ok())
                                .map(|p| p.timestamp)
                                .unwrap_or(0);
                            if profile.timestamp > existing_ts
                                && let Ok(bytes) = postcard::to_allocvec(profile)
                            {
                                self.store.set(&peer_profile_key(&profile.peer_id), bytes);
                            }
                        }
                        self.inner.on_profiles_backfilled(profiles);
                    }
                }

                CONTENT_TYPE_PARTY => {
                    if let Ok(party) = postcard::from_bytes::<PartyProfile>(payload) {
                        save_if_newer(
                            &self.store,
                            &party_key(party.id),
                            &party,
                            party.timestamp,
                            |p| p.timestamp,
                        );
                        self.inner.on_party_received(party);
                    }
                }

                CONTENT_TYPE_PARTY_REPLY => {
                    if let Ok(party) = postcard::from_bytes::<PartyProfile>(payload) {
                        save_if_newer(
                            &self.store,
                            &party_key(party.id),
                            &party,
                            party.timestamp,
                            |p| p.timestamp,
                        );
                        self.inner.on_party_received(party);
                    }
                }

                CONTENT_TYPE_PARTY_BACKFILL => {
                    if let Ok(parties) = postcard::from_bytes::<Vec<PartyProfile>>(payload) {
                        for party in &parties {
                            save_if_newer(
                                &self.store,
                                &party_key(party.id),
                                party,
                                party.timestamp,
                                |p| p.timestamp,
                            );
                        }
                        self.inner.on_parties_backfilled(parties);
                    }
                }

                CONTENT_TYPE_ALERT => {
                    if let Ok(alert) = postcard::from_bytes::<AlertProfile>(payload) {
                        save_if_newer(
                            &self.store,
                            &alert_key(alert.id),
                            &alert,
                            alert.timestamp,
                            |a| a.timestamp,
                        );
                        self.inner.on_alert_received(alert);
                    }
                }

                CONTENT_TYPE_ALERT_REPLY => {
                    if let Ok(alert) = postcard::from_bytes::<AlertProfile>(payload) {
                        save_if_newer(
                            &self.store,
                            &alert_key(alert.id),
                            &alert,
                            alert.timestamp,
                            |a| a.timestamp,
                        );
                        self.inner.on_alert_received(alert);
                    }
                }

                CONTENT_TYPE_ALERT_BACKFILL => {
                    if let Ok(alerts) = postcard::from_bytes::<Vec<AlertProfile>>(payload) {
                        for alert in &alerts {
                            save_if_newer(
                                &self.store,
                                &alert_key(alert.id),
                                alert,
                                alert.timestamp,
                                |a| a.timestamp,
                            );
                        }
                        self.inner.on_alerts_backfilled(alerts);
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
                let store = Arc::clone(&self.store);
                let peer_id_str = peer_id.to_string();
                let tx_clone = tx.clone();

                tokio::spawn(async move {
                    let last_seen = store
                        .get(&peer_last_seen_key(&peer_id_str))
                        .and_then(|b| b.try_into().ok())
                        .map(u64::from_le_bytes)
                        .unwrap_or(0);

                    let profile_payload = postcard::to_allocvec(&our_profile).unwrap_or_default();
                    ZRPHandle::send_typed(
                        tx_clone.clone(),
                        "fratrat/v1/profiles".to_string(),
                        profile_payload,
                        CONTENT_TYPE_PROFILE,
                    )
                    .await;

                    let missed_profiles: Vec<PeerProfile> = store
                        .list(PEER_PROFILE_PREFIX)
                        .into_iter()
                        .filter_map(|key| store.get(&key))
                        .filter_map(|bytes| postcard::from_bytes::<PeerProfile>(&bytes).ok())
                        .filter(|p| p.peer_id != peer_id_str)
                        .filter(|p| p.timestamp > last_seen)
                        .collect();

                    if !missed_profiles.is_empty() {
                        tracing::info!(
                            "backfilling {} profiles to {} (last seen: {})",
                            missed_profiles.len(),
                            peer_id_str,
                            last_seen
                        );
                        let payload = postcard::to_allocvec(&missed_profiles).unwrap_or_default();
                        ZRPHandle::send_typed(
                            tx_clone.clone(),
                            format!("fratrat/v1/profiles/dm/{}", peer_id_str),
                            payload,
                            CONTENT_TYPE_PROFILE_BACKFILL,
                        )
                        .await;
                    }

                    let missed_parties: Vec<PartyProfile> = store
                        .list(PARTY_PREFIX)
                        .into_iter()
                        .filter_map(|key| store.get(&key))
                        .filter_map(|bytes| postcard::from_bytes::<PartyProfile>(&bytes).ok())
                        .filter(|p| p.timestamp > last_seen)
                        .collect();

                    if !missed_parties.is_empty() {
                        tracing::info!(
                            "backfilling {} parties to {} (last seen: {})",
                            missed_parties.len(),
                            peer_id_str,
                            last_seen
                        );
                        let payload = postcard::to_allocvec(&missed_parties).unwrap_or_default();
                        ZRPHandle::send_typed(
                            tx_clone.clone(),
                            format!("fratrat/v1/parties/dm/{}", peer_id_str),
                            payload,
                            CONTENT_TYPE_PARTY_BACKFILL,
                        )
                        .await;
                    }

                    let missed_alerts: Vec<AlertProfile> = store
                        .list(ALERT_PREFIX)
                        .into_iter()
                        .filter_map(|key| store.get(&key))
                        .filter_map(|bytes| postcard::from_bytes::<AlertProfile>(&bytes).ok())
                        .filter(|a| a.timestamp > last_seen)
                        .collect();

                    if !missed_alerts.is_empty() {
                        tracing::info!(
                            "backfilling {} alerts to {} (last seen: {})",
                            missed_alerts.len(),
                            peer_id_str,
                            last_seen
                        );
                        let payload = postcard::to_allocvec(&missed_alerts).unwrap_or_default();
                        ZRPHandle::send_typed(
                            tx_clone,
                            format!("fratrat/v1/alerts/dm/{}", peer_id_str),
                            payload,
                            CONTENT_TYPE_ALERT_BACKFILL,
                        )
                        .await;
                    }
                });
            }

            ZRPEvent::PeerDisconnected { peer_id, .. } => {
                let ts = now_unix().to_le_bytes().to_vec();
                self.store
                    .set(&peer_last_seen_key(&peer_id.to_string()), ts);
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

    #[uniffi::constructor]
    pub fn load_or_generate_default() -> Self {
        let store = store();
        Self {
            inner: CoreIdentity::load_or_generate(&store),
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
            inner: CoreIdentity::load_or_generate(&store()),
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
        tracing::info!("MeshNode::new relay_addr={}", relay_addr);

        let core_identity = CoreIdentity::from_keypair_bytes(&identity.inner.to_keypair_bytes())
            .map_err(MeshError::from)?;

        let core_config = CoreConfig::with_ports(config.inner.tcp_port, config.inner.quic_port);

        let store = store();

        let bridge = SwiftHandlerBridge {
            inner: Arc::from(handler),
            profile: Arc::new(std::sync::Mutex::new(profile)),
            store: Arc::clone(&store),
        };

        let handle = get_runtime()
            .spawn(async move {
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
        handle
            .subscribe(format!("fratrat/v1/parties/dm/{}", my_peer_id))
            .await;
        handle
            .subscribe(format!("fratrat/v1/alerts/dm/{}", my_peer_id))
            .await;
        handle.subscribe("fratrat/v1/profiles".to_string()).await;
        handle.subscribe("fratrat/v1/parties".to_string()).await;
        handle.subscribe("fratrat/v1/alerts".to_string()).await;

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

    pub async fn announce_profile(&self, profile: PeerProfile) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&profile)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;
            h.publish_typed(
                "fratrat/v1/profiles".to_string(),
                bytes,
                CONTENT_TYPE_PROFILE,
            )
            .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn reply_with_profile(
        &self,
        profile: PeerProfile,
        to_peer_id: String,
    ) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&profile)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;
            let topic = format!("fratrat/v1/profiles/dm/{}", to_peer_id);
            h.publish_typed(topic, bytes, CONTENT_TYPE_PROFILE_REPLY)
                .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn announce_party(&self, party: PartyProfile) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&party)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;

            if let Ok(b) = postcard::to_allocvec(&party) {
                store().set(&party_key(party.id), b);
            }
            h.publish_typed("fratrat/v1/parties".to_string(), bytes, CONTENT_TYPE_PARTY)
                .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn announce_alert(&self, alert: AlertProfile) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&alert)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;
            if let Ok(b) = postcard::to_allocvec(&alert) {
                store().set(&alert_key(alert.id), b);
            }
            h.publish_typed("fratrat/v1/alerts".to_string(), bytes, CONTENT_TYPE_ALERT)
                .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn upload_photo(&self, data: Vec<u8>) -> Result<String, MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.upload_blob(data).await.map_err(MeshError::from)
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn retrieve_photo(&self, blob_id: String) -> Result<Vec<u8>, MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.retrieve_blob(&blob_id).await.map_err(MeshError::from)
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn blob_storage_bytes(&self) -> u64 {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.blob_storage_bytes()
        } else {
            0
        }
    }

    pub async fn evict_old_blobs(&self, max_age_secs: u64) {
        if let Some(h) = self.handle.lock().await.as_ref() {
            h.evict_old_blobs(max_age_secs);
        }
    }

    pub async fn shutdown(&self) {
        if let Some(h) = self.handle.lock().await.take() {
            h.shutdown().await;
        }
    }
}

#[uniffi::export]
pub fn discover_relays(grpc_addr: String) -> Vec<RelayInfo> {
    get_runtime().block_on(async move {
        match libghost::relay::RelayClient::connect(&grpc_addr).await {
            Ok(mut client) => {
                tracing::info!("discover_relays: connected to {}", grpc_addr);
                client
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
                    .collect()
            }
            Err(e) => {
                tracing::error!("discover_relays: failed for '{}': {:?}", grpc_addr, e);
                vec![]
            }
        }
    })
}

#[derive(uniffi::Record)]
pub struct RelayInfo {
    pub multiaddr: String,
    pub peer_id: String,
    pub region: String,
    pub load: u32,
    pub meshes: Vec<String>,
}
