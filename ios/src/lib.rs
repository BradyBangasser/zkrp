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

// MARK: - PeerProfile

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

// MARK: - PartyProfile

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

// MARK: - AlertProfile

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

// MARK: - Store key helpers

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

/// Generic helper: save any serializable item, only overwrite if newer.
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
    if item_ts > existing_ts {
        if let Ok(bytes) = postcard::to_allocvec(item) {
            store.set(key, bytes);
            return true;
        }
    }
    false
}

// MARK: - Own profile persistence

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

// MARK: - Peer profile cache (other peers we've seen)

/// Persist a peer's profile to SQLite. Called automatically by
/// SwiftHandlerBridge when a profile is received, so Swift doesn't
/// need to manage this manually.
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

/// Load a single peer's cached profile by peer ID.
#[uniffi::export]
pub fn load_peer_profile(peer_id: String) -> Option<PeerProfile> {
    let store = store();
    let bytes = store.get(&peer_profile_key(&peer_id))?;
    postcard::from_bytes::<PeerProfile>(&bytes).ok()
}

/// Load ALL cached peer profiles from SQLite. Called on app launch
/// so the UI is immediately populated before any network activity.
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

/// Load all peer profiles with timestamp > since_unix_secs.
/// Used for backfill: when peer B connects, we send them all profiles
/// of peers who joined while B was offline.
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

// MARK: - Peer last-seen tracking

/// Record the current time as the last-seen timestamp for a peer.
/// Called on PeerDisconnected so we know exactly when they left.
#[uniffi::export]
pub fn update_peer_last_seen(peer_id: String) {
    let store = store();
    let ts = now_unix().to_le_bytes().to_vec();
    store.set(&peer_last_seen_key(&peer_id), ts);
}

/// Get the unix timestamp when a peer was last seen online.
/// Returns 0 if the peer has never been seen (first-ever connection).
#[uniffi::export]
pub fn get_peer_last_seen(peer_id: String) -> u64 {
    let store = store();
    store
        .get(&peer_last_seen_key(&peer_id))
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

// MARK: - Party persistence

/// Save a party to the local cache (keyed by party ID).
/// Overwrites only if the incoming timestamp is newer.
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

/// Load a single cached party by ID.
#[uniffi::export]
pub fn load_party(id: u64) -> Option<PartyProfile> {
    let store = store();
    let bytes = store.get(&party_key(id))?;
    postcard::from_bytes::<PartyProfile>(&bytes).ok()
}

/// Load all cached parties (including expired ones — let Swift filter).
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

/// Load parties posted/updated after since_unix_secs (for backfill).
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

/// Delete a party from the cache (e.g. after it expires).
#[uniffi::export]
pub fn delete_party(id: u64) {
    store().delete(&party_key(id));
}

// MARK: - Alert persistence

/// Save an alert to the local cache (keyed by alert ID).
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

/// Load a single cached alert by ID.
#[uniffi::export]
pub fn load_alert(id: u64) -> Option<AlertProfile> {
    let store = store();
    let bytes = store.get(&alert_key(id))?;
    postcard::from_bytes::<AlertProfile>(&bytes).ok()
}

/// Load all cached alerts (including expired — let Swift filter).
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

/// Load alerts posted after since_unix_secs (for backfill).
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

/// Delete an alert from the cache.
#[uniffi::export]
pub fn delete_alert(id: u64) {
    store().delete(&alert_key(id));
}

// MARK: - Message persistence

pub const CONTENT_TYPE_MESSAGE: u16 = 0x0040;
pub const CONTENT_TYPE_MESSAGE_BACKFILL: u16 = 0x0041;

// ── Like flow (always DM, always encrypted to recipient pubkey) ──────────────
pub const CONTENT_TYPE_LIKE: u16 = 0x0050;
pub const CONTENT_TYPE_LIKE_BACK: u16 = 0x0051;
pub const CONTENT_TYPE_LIKE_DECLINED: u16 = 0x0052;

// ── Direct invite flow (DM, encrypted) ──────────────────────────────────────
pub const CONTENT_TYPE_INVITE: u16 = 0x0060;
pub const CONTENT_TYPE_INVITE_ACCEPT: u16 = 0x0061;
pub const CONTENT_TYPE_INVITE_DECLINE: u16 = 0x0062;

const MESSAGE_PREFIX: &str = "fratrat/msg/";
const RELATIONSHIP_PREFIX: &str = "fratrat/rel/";

/// A stored chat message. `message_id` is a stable UUID string that
/// survives serialization — used for dedup on both sides.
#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct StoredMessage {
    pub message_id: String,      // UUID string — stable identity for dedup
    pub conversation_id: String, // peer_id of the other party (or invite id)
    pub sender_peer_id: String,  // who sent it
    pub text: String,
    pub timestamp: u64,
    pub is_own: bool, // true if we sent this message
}

fn message_key(conversation_id: &str, message_id: &str) -> String {
    format!("{}{}/{}", MESSAGE_PREFIX, conversation_id, message_id)
}

fn message_prefix(conversation_id: &str) -> String {
    format!("{}{}/", MESSAGE_PREFIX, conversation_id)
}

/// Persist a message to SQLite.
#[uniffi::export]
pub fn save_message(message: StoredMessage) -> bool {
    let store = store();
    match postcard::to_allocvec(&message) {
        Ok(bytes) => {
            store.set(
                &message_key(&message.conversation_id, &message.message_id),
                bytes,
            );
            true
        }
        Err(e) => {
            tracing::error!("save_message: {}", e);
            false
        }
    }
}

/// Load all messages for a conversation, sorted by timestamp ascending.
#[uniffi::export]
pub fn load_messages(conversation_id: String) -> Vec<StoredMessage> {
    let store = store();
    let mut msgs: Vec<StoredMessage> = store
        .list(&message_prefix(&conversation_id))
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<StoredMessage>(&bytes).ok())
        .collect();
    msgs.sort_by_key(|m| m.timestamp);
    msgs
}

/// Load messages for a conversation newer than since_unix_secs.
/// Used for backfill: send the other peer messages they missed.
#[uniffi::export]
pub fn load_messages_since(conversation_id: String, since_unix_secs: u64) -> Vec<StoredMessage> {
    let store = store();
    let mut msgs: Vec<StoredMessage> = store
        .list(&message_prefix(&conversation_id))
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<StoredMessage>(&bytes).ok())
        .filter(|m| m.timestamp > since_unix_secs)
        .collect();
    msgs.sort_by_key(|m| m.timestamp);
    msgs
}

/// Load all conversation IDs that have at least one stored message.
/// Used to seed the conversations list on app launch.
#[uniffi::export]
pub fn load_all_conversation_ids() -> Vec<String> {
    let store = store();
    let mut ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for key in store.list(MESSAGE_PREFIX) {
        // key format: "fratrat/msg/<conversation_id>/<message_id>"
        let suffix = key.trim_start_matches(MESSAGE_PREFIX);
        if let Some(slash) = suffix.find('/') {
            ids.insert(suffix[..slash].to_string());
        }
    }
    ids.into_iter().collect()
}

/// Delete all messages for a conversation (e.g. user clears history).
#[uniffi::export]
pub fn delete_conversation(conversation_id: String) {
    let store = store();
    for key in store.list(&message_prefix(&conversation_id)) {
        store.delete(&key);
    }
}

// MARK: - Relationship state

/// Tracks the relationship between us and a specific peer.
/// Stored in SQLite under "fratrat/rel/<peer_id>".
/// The state machine enforces the invite/like guard rules:
///   - Can only like/invite a peer in state None
///   - Group chats bypass this check (handled at call site)
#[derive(uniffi::Enum, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum RelationshipState {
    /// No relationship yet
    None,
    /// We swiped right — waiting to see if they like us back
    Liked,
    /// They liked us — key encrypted to our pubkey, waiting for us to act
    LikedBy { conversation_key_enc: Vec<u8> },
    /// Mutual like — conversation open
    Matched { conversation_id: String },
    /// We sent them a direct invite — waiting for response
    InviteSent { conversation_id: String },
    /// They invited us — waiting for our response
    InviteReceived {
        conversation_id: String,
        from_name: String,
        conversation_key_enc: Vec<u8>,
    },
    /// We accepted their invite — conversation open
    InviteAccepted { conversation_id: String },
    /// Declined (either direction)
    Declined,
}

fn relationship_key(peer_id: &str) -> String {
    format!("{}{}", RELATIONSHIP_PREFIX, peer_id)
}

#[uniffi::export]
pub fn save_relationship(peer_id: String, state: RelationshipState) -> bool {
    let store = store();
    match postcard::to_allocvec(&state) {
        Ok(bytes) => {
            store.set(&relationship_key(&peer_id), bytes);
            true
        }
        Err(e) => {
            tracing::error!("save_relationship: {}", e);
            false
        }
    }
}

#[uniffi::export]
pub fn load_relationship(peer_id: String) -> RelationshipState {
    let store = store();
    store
        .get(&relationship_key(&peer_id))
        .and_then(|b| postcard::from_bytes::<RelationshipState>(&b).ok())
        .unwrap_or(RelationshipState::None)
}

/// Load all non-None relationships — used to seed UI on launch.
#[uniffi::export]
pub fn load_all_relationships() -> Vec<PeerRelationship> {
    let store = store();
    store
        .list(RELATIONSHIP_PREFIX)
        .into_iter()
        .filter_map(|key| {
            let peer_id = key.trim_start_matches(RELATIONSHIP_PREFIX).to_string();
            let bytes = store.get(&key)?;
            let state = postcard::from_bytes::<RelationshipState>(&bytes).ok()?;
            Some(PeerRelationship { peer_id, state })
        })
        .collect()
}

/// Whether we can send a like to this peer (None state only).
#[uniffi::export]
pub fn can_like(peer_id: String) -> bool {
    matches!(load_relationship(peer_id), RelationshipState::None)
}

/// Whether we can send a direct invite to this peer.
/// Blocked if we've already liked, been matched, sent an invite,
/// or already accepted an invite. Still allowed if they liked us
/// (we can invite instead of matching) or if we declined.
#[uniffi::export]
pub fn can_invite(peer_id: String) -> bool {
    matches!(
        load_relationship(peer_id),
        RelationshipState::None | RelationshipState::LikedBy { .. } | RelationshipState::Declined
    )
}

/// Flat record for FFI — pairs a peer_id with their RelationshipState.
#[derive(uniffi::Record, Clone)]
pub struct PeerRelationship {
    pub peer_id: String,
    pub state: RelationshipState,
}

// MARK: - Like and Invite payloads

/// Sent as content_type 0x0050 (LIKE) over the peer's DM topic.
/// conversation_key_enc is the proposed conversation key encrypted
/// to the recipient's libp2p public key via encrypt_for_peer().
/// The relay sees only ciphertext; neither key nor intent is exposed.
#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct LikePayload {
    pub conversation_key_enc: Vec<u8>,
    pub sender_peer_id: String,
    pub timestamp: u64,
}

/// Sent as content_type 0x0051 (LIKE_BACK) — confirms mutual match.
/// Contains the same conversation_key_enc (re-encrypted to sender)
/// so both sides end up with the same decrypted conversation key.
#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct LikeBackPayload {
    pub conversation_id: String,
    pub conversation_key_enc: Vec<u8>, // encrypted back to original sender
    pub sender_peer_id: String,
    pub timestamp: u64,
}

/// Sent as content_type 0x0060 (INVITE) over the peer's DM topic.
#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct InvitePayload {
    pub conversation_id: String,
    pub conversation_key_enc: Vec<u8>, // encrypted to recipient's pubkey
    pub sender_name: String,
    pub sender_peer_id: String,
    pub timestamp: u64,
}

/// Sent as content_type 0x0061 (INVITE_ACCEPT).
#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct InviteAcceptPayload {
    pub conversation_id: String,
    pub acceptor_peer_id: String,
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
    // ── Profile callbacks ────────────────────────────────────────────
    fn on_profile_received(&self, profile: PeerProfile);
    fn on_profiles_backfilled(&self, profiles: Vec<PeerProfile>);
    // ── Party callbacks ──────────────────────────────────────────────
    fn on_party_received(&self, party: PartyProfile);
    fn on_parties_backfilled(&self, parties: Vec<PartyProfile>);
    // ── Alert callbacks ──────────────────────────────────────────────
    fn on_alert_received(&self, alert: AlertProfile);
    fn on_alerts_backfilled(&self, alerts: Vec<AlertProfile>);
    // ── Message callbacks ────────────────────────────────────────────
    fn on_message_received(&self, message: StoredMessage);
    fn on_messages_backfilled(&self, messages: Vec<StoredMessage>);
    // ── Like callbacks ───────────────────────────────────────────────
    fn on_like_received(&self, payload: LikePayload);
    fn on_like_back_received(&self, payload: LikeBackPayload);
    fn on_like_declined(&self, peer_id: String);
    // ── Invite callbacks ─────────────────────────────────────────────
    fn on_invite_received(&self, payload: InvitePayload);
    fn on_invite_accepted(&self, payload: InviteAcceptPayload);
    fn on_invite_declined(&self, peer_id: String);
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
                // ── Incoming profile announcement ────────────────────────
                // Peer is announcing themselves. We:
                // 1. Fire on_profile_received so Swift updates the UI
                // 2. Persist to our SQLite cache
                // 3. Reply with our own profile via DM topic
                CONTENT_TYPE_PROFILE => {
                    if let Ok(profile) = postcard::from_bytes::<PeerProfile>(payload) {
                        // Persist to cache
                        if let Ok(bytes) = postcard::to_allocvec(&profile) {
                            self.store.set(&peer_profile_key(&profile.peer_id), bytes);
                        }
                        self.inner.on_profile_received(profile.clone());

                        // Reply with our profile
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

                // ── Direct profile reply ─────────────────────────────────
                // Response to our announcement — just store and notify.
                CONTENT_TYPE_PROFILE_REPLY => {
                    if let Ok(profile) = postcard::from_bytes::<PeerProfile>(payload) {
                        if let Ok(bytes) = postcard::to_allocvec(&profile) {
                            self.store.set(&peer_profile_key(&profile.peer_id), bytes);
                        }
                        self.inner.on_profile_received(profile);
                    }
                }

                // ── Backfill batch ───────────────────────────────────────
                // A peer is sending us profiles of peers we missed while
                // offline. Store all of them and notify Swift in one call.
                CONTENT_TYPE_PROFILE_BACKFILL => {
                    if let Ok(profiles) = postcard::from_bytes::<Vec<PeerProfile>>(payload) {
                        for profile in &profiles {
                            let existing_ts = self
                                .store
                                .get(&peer_profile_key(&profile.peer_id))
                                .and_then(|b| postcard::from_bytes::<PeerProfile>(&b).ok())
                                .map(|p| p.timestamp)
                                .unwrap_or(0);
                            if profile.timestamp > existing_ts {
                                if let Ok(bytes) = postcard::to_allocvec(profile) {
                                    self.store.set(&peer_profile_key(&profile.peer_id), bytes);
                                }
                            }
                        }
                        self.inner.on_profiles_backfilled(profiles);
                    }
                }

                // ── Party announce ───────────────────────────────────────
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

                // ── Party reply ──────────────────────────────────────────
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

                // ── Party backfill ───────────────────────────────────────
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

                // ── Alert announce ───────────────────────────────────────
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

                // ── Alert reply ──────────────────────────────────────────
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

                // ── Alert backfill ───────────────────────────────────────
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

                // ── Chat message ─────────────────────────────────────────
                // Persist immediately so it survives app restarts, then
                // notify Swift. Dedup by message_id on the Swift side.
                CONTENT_TYPE_MESSAGE => {
                    if let Ok(msg) = postcard::from_bytes::<StoredMessage>(payload) {
                        let key = message_key(&msg.conversation_id, &msg.message_id);
                        if self.store.get(&key).is_none() {
                            if let Ok(bytes) = postcard::to_allocvec(&msg) {
                                self.store.set(&key, bytes);
                            }
                        }
                        self.inner.on_message_received(msg);
                    }
                }

                // ── Message backfill ─────────────────────────────────────
                // Batch of messages the peer missed while offline.
                // Only store messages we haven't seen before.
                CONTENT_TYPE_MESSAGE_BACKFILL => {
                    if let Ok(msgs) = postcard::from_bytes::<Vec<StoredMessage>>(payload) {
                        let new_msgs: Vec<StoredMessage> = msgs
                            .into_iter()
                            .filter(|msg| {
                                let key = message_key(&msg.conversation_id, &msg.message_id);
                                if self.store.get(&key).is_none() {
                                    if let Ok(bytes) = postcard::to_allocvec(msg) {
                                        self.store.set(&key, bytes);
                                    }
                                    true
                                } else {
                                    false
                                }
                            })
                            .collect();
                        if !new_msgs.is_empty() {
                            self.inner.on_messages_backfilled(new_msgs);
                        }
                    }
                }

                // ── Like received ─────────────────────────────────────────
                // Store relationship state as LikedBy so we know to
                // show an incoming like notification and can later
                // match or decline. The encrypted key stays in the
                // relationship record until the user acts on it.
                CONTENT_TYPE_LIKE => {
                    if let Ok(like) = postcard::from_bytes::<LikePayload>(payload) {
                        let state = RelationshipState::LikedBy {
                            conversation_key_enc: like.conversation_key_enc.clone(),
                        };
                        if let Ok(bytes) = postcard::to_allocvec(&state) {
                            self.store
                                .set(&relationship_key(&like.sender_peer_id), bytes);
                        }
                        self.inner.on_like_received(like);
                    }
                }

                // ── Like back (mutual match) ──────────────────────────────
                // Update our state to Matched — the conversation is open.
                CONTENT_TYPE_LIKE_BACK => {
                    if let Ok(payload_inner) = postcard::from_bytes::<LikeBackPayload>(payload) {
                        let state = RelationshipState::Matched {
                            conversation_id: payload_inner.conversation_id.clone(),
                        };
                        if let Ok(bytes) = postcard::to_allocvec(&state) {
                            self.store
                                .set(&relationship_key(&payload_inner.sender_peer_id), bytes);
                        }
                        self.inner.on_like_back_received(payload_inner);
                    }
                }

                // ── Like declined ─────────────────────────────────────────
                CONTENT_TYPE_LIKE_DECLINED => {
                    let peer_id_str = peer_id.to_string();
                    let state = RelationshipState::Declined;
                    if let Ok(bytes) = postcard::to_allocvec(&state) {
                        self.store.set(&relationship_key(&peer_id_str), bytes);
                    }
                    self.inner.on_like_declined(peer_id_str);
                }

                // ── Direct invite received ────────────────────────────────
                // Store as InviteReceived — Swift shows an incoming
                // invite notification with Accept / Decline.
                CONTENT_TYPE_INVITE => {
                    if let Ok(invite) = postcard::from_bytes::<InvitePayload>(payload) {
                        let state = RelationshipState::InviteReceived {
                            conversation_id: invite.conversation_id.clone(),
                            from_name: invite.sender_name.clone(),
                            conversation_key_enc: invite.conversation_key_enc.clone(),
                        };
                        if let Ok(bytes) = postcard::to_allocvec(&state) {
                            self.store
                                .set(&relationship_key(&invite.sender_peer_id), bytes);
                        }
                        self.inner.on_invite_received(invite);
                    }
                }

                // ── Invite accepted ───────────────────────────────────────
                // Update our InviteSent state to InviteAccepted.
                CONTENT_TYPE_INVITE_ACCEPT => {
                    if let Ok(accept) = postcard::from_bytes::<InviteAcceptPayload>(payload) {
                        let state = RelationshipState::InviteAccepted {
                            conversation_id: accept.conversation_id.clone(),
                        };
                        if let Ok(bytes) = postcard::to_allocvec(&state) {
                            self.store
                                .set(&relationship_key(&accept.acceptor_peer_id), bytes);
                        }
                        self.inner.on_invite_accepted(accept);
                    }
                }

                // ── Invite declined ───────────────────────────────────────
                CONTENT_TYPE_INVITE_DECLINE => {
                    let peer_id_str = peer_id.to_string();
                    let state = RelationshipState::Declined;
                    if let Ok(bytes) = postcard::to_allocvec(&state) {
                        self.store.set(&relationship_key(&peer_id_str), bytes);
                    }
                    self.inner.on_invite_declined(peer_id_str);
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
                        .unwrap_or(0); // 0 = never seen, send everything

                    // 1. Announce our own profile
                    let profile_payload = postcard::to_allocvec(&our_profile).unwrap_or_default();
                    ZRPHandle::send_typed(
                        tx_clone.clone(),
                        "fratrat/v1/profiles".to_string(),
                        profile_payload,
                        CONTENT_TYPE_PROFILE,
                    )
                    .await;

                    // 2. Backfill peer profiles missed while offline
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

                    // 3. Backfill parties missed while offline
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

                    // 4. Backfill alerts missed while offline
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
                            tx_clone.clone(),
                            format!("fratrat/v1/alerts/dm/{}", peer_id_str),
                            payload,
                            CONTENT_TYPE_ALERT_BACKFILL,
                        )
                        .await;
                    }

                    // 5. Backfill messages from our shared conversation
                    //    that the peer missed while offline.
                    //    We only have access to messages where we were
                    //    one of the parties — use peer_id as conversation_id.
                    let missed_messages: Vec<StoredMessage> = store
                        .list(&message_prefix(&peer_id_str))
                        .into_iter()
                        .filter_map(|key| store.get(&key))
                        .filter_map(|bytes| postcard::from_bytes::<StoredMessage>(&bytes).ok())
                        .filter(|m| m.timestamp > last_seen)
                        .collect();

                    if !missed_messages.is_empty() {
                        tracing::info!(
                            "backfilling {} messages to {} (last seen: {})",
                            missed_messages.len(),
                            peer_id_str,
                            last_seen
                        );
                        let payload = postcard::to_allocvec(&missed_messages).unwrap_or_default();
                        ZRPHandle::send_typed(
                            tx_clone.clone(),
                            format!("fratrat/v1/dm/{}", peer_id_str),
                            payload,
                            CONTENT_TYPE_MESSAGE_BACKFILL,
                        )
                        .await;
                    }

                    // 6. Re-send any pending like or invite to this peer
                    //    that they missed while offline.
                    let pending_like_key = format!("fratrat/pending_like/{}", peer_id_str);
                    if let Some(like_bytes) = store.get(&pending_like_key) {
                        tracing::info!("re-sending pending like to {}", peer_id_str);
                        ZRPHandle::send_typed(
                            tx_clone.clone(),
                            format!("fratrat/v1/dm/{}", peer_id_str),
                            like_bytes,
                            CONTENT_TYPE_LIKE,
                        )
                        .await;
                    }

                    let pending_invite_key = format!("fratrat/pending_invite/{}", peer_id_str);
                    if let Some(inv_bytes) = store.get(&pending_invite_key) {
                        tracing::info!("re-sending pending invite to {}", peer_id_str);
                        ZRPHandle::send_typed(
                            tx_clone,
                            format!("fratrat/v1/dm/{}", peer_id_str),
                            inv_bytes,
                            CONTENT_TYPE_INVITE,
                        )
                        .await;
                    }
                });
                // Record last-seen so future backfills know the window
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

    /// Encrypt plaintext so only the holder of peer_id's private key
    /// can decrypt it. Returns [ephemeral_pubkey(32) || nonce(12) || ciphertext].
    pub fn encrypt_for_peer(
        &self,
        plaintext: Vec<u8>,
        peer_id: String,
    ) -> Result<Vec<u8>, MeshError> {
        self.inner
            .encrypt_for_peer(&plaintext, &peer_id)
            .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })
    }

    /// Decrypt a value encrypted by encrypt_for_peer using our private key.
    pub fn decrypt(&self, ciphertext: Vec<u8>) -> Result<Vec<u8>, MeshError> {
        self.inner
            .decrypt(&ciphertext)
            .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })
    }
}

/// Generate 32 random bytes suitable for use as a conversation key.
/// Free function so Swift can call it as generateConversationKey()
/// without needing a NodeIdentity instance.
#[uniffi::export]
pub fn generate_conversation_key() -> Vec<u8> {
    CoreIdentity::generate_conversation_key()
}

impl Default for NodeIdentity {
    fn default() -> Self {
        Self {
            inner: CoreIdentity::load_or_generate(&store()),
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
        handle
            .subscribe(format!("fratrat/v1/dm/{}", my_peer_id))
            .await;
        handle.subscribe("fratrat/v1/profiles".to_string()).await;
        handle.subscribe("fratrat/v1/parties".to_string()).await;
        handle.subscribe("fratrat/v1/alerts".to_string()).await;

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

    // MARK: - Profile

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

    /// Broadcast a party to the mesh. Also persists to local SQLite
    /// so it's included in backfills to peers who were offline.
    pub async fn announce_party(&self, party: PartyProfile) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&party)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;
            // Persist locally first so we can backfill it to late-joining peers
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

    /// Broadcast an alert to the mesh. Also persists to local SQLite.
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

    // MARK: - Like flow

    pub async fn send_like(
        &self,
        to_peer_id: String,
        payload: LikePayload,
    ) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&payload)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;
            // Persist as pending keyed by recipient so backfill can find it
            store().set(
                &format!("fratrat/pending_like/{}", to_peer_id),
                bytes.clone(),
            );
            // Track relationship state keyed by recipient
            let state = RelationshipState::Liked;
            if let Ok(sb) = postcard::to_allocvec(&state) {
                store().set(&relationship_key(&to_peer_id), sb);
            }
            h.publish_typed(
                format!("fratrat/v1/dm/{}", to_peer_id),
                bytes,
                CONTENT_TYPE_LIKE,
            )
            .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn send_like_back(
        &self,
        to_peer_id: String,
        payload: LikeBackPayload,
    ) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&payload)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;
            let state = RelationshipState::Matched {
                conversation_id: payload.conversation_id.clone(),
            };
            if let Ok(sb) = postcard::to_allocvec(&state) {
                store().set(&relationship_key(&to_peer_id), sb);
            }
            h.publish_typed(
                format!("fratrat/v1/dm/{}", to_peer_id),
                bytes,
                CONTENT_TYPE_LIKE_BACK,
            )
            .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn send_like_declined(&self, to_peer_id: String) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let state = RelationshipState::Declined;
            if let Ok(sb) = postcard::to_allocvec(&state) {
                store().set(&relationship_key(&to_peer_id), sb);
            }
            h.publish_typed(
                format!("fratrat/v1/dm/{}", to_peer_id),
                vec![],
                CONTENT_TYPE_LIKE_DECLINED,
            )
            .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn send_invite(
        &self,
        to_peer_id: String,
        payload: InvitePayload,
    ) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&payload)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;
            store().set(
                &format!("fratrat/pending_invite/{}", to_peer_id),
                bytes.clone(),
            );
            let state = RelationshipState::InviteSent {
                conversation_id: payload.conversation_id.clone(),
            };
            if let Ok(sb) = postcard::to_allocvec(&state) {
                store().set(&relationship_key(&to_peer_id), sb);
            }
            h.publish_typed(
                format!("fratrat/v1/dm/{}", to_peer_id),
                bytes,
                CONTENT_TYPE_INVITE,
            )
            .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn send_invite_accept(
        &self,
        to_peer_id: String,
        payload: InviteAcceptPayload,
    ) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let bytes = postcard::to_allocvec(&payload)
                .map_err(|e| MeshError::ConnectionError { msg: e.to_string() })?;
            let state = RelationshipState::InviteAccepted {
                conversation_id: payload.conversation_id.clone(),
            };
            if let Ok(sb) = postcard::to_allocvec(&state) {
                store().set(&relationship_key(&to_peer_id), sb);
            }
            h.publish_typed(
                format!("fratrat/v1/dm/{}", to_peer_id),
                bytes,
                CONTENT_TYPE_INVITE_ACCEPT,
            )
            .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    pub async fn send_invite_decline(&self, to_peer_id: String) -> Result<(), MeshError> {
        if let Some(h) = self.handle.lock().await.as_ref() {
            let state = RelationshipState::Declined;
            if let Ok(sb) = postcard::to_allocvec(&state) {
                store().set(&relationship_key(&to_peer_id), sb);
            }
            store().delete(&format!("fratrat/pending_invite/{}", to_peer_id));
            h.publish_typed(
                format!("fratrat/v1/dm/{}", to_peer_id),
                vec![],
                CONTENT_TYPE_INVITE_DECLINE,
            )
            .await;
            Ok(())
        } else {
            Err(MeshError::ConnectionFailed)
        }
    }

    // MARK: - Blob / photo storage

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

// MARK: - Records

#[derive(uniffi::Record)]
pub struct RelayInfo {
    pub multiaddr: String,
    pub peer_id: String,
    pub region: String,
    pub load: u32,
    pub meshes: Vec<String>,
}
