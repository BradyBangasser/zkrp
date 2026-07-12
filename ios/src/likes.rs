//! Like flow wire types.
//!
//! Extracted from `lib.rs`. This holds the parts of the like flow that stand on
//! their own — the content types and the two encrypted payloads. The broader
//! relationship state machine (`RelationshipState`) stays in `lib.rs` because it
//! is shared with invites and messages; the swipe handlers and send paths live
//! there too.
//!
//! Likes are always DM'd and encrypted to the recipient's key
//! (`identity::encrypt_for_peer`), so the relay sees only ciphertext — neither
//! the conversation key nor the intent is exposed to it.

// ── Like flow content types (always DM, encrypted to recipient pubkey) ───────
pub const CONTENT_TYPE_LIKE: u16 = 0x0050;
pub const CONTENT_TYPE_LIKE_BACK: u16 = 0x0051;
pub const CONTENT_TYPE_LIKE_DECLINED: u16 = 0x0052;

/// Sent as content_type 0x0050 (LIKE) over the peer's DM topic.
/// `conversation_key_enc` is the proposed conversation key encrypted to the
/// recipient's libp2p public key via `encrypt_for_peer()`. The relay sees only
/// ciphertext; neither key nor intent is exposed.
#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct LikePayload {
    pub conversation_key_enc: Vec<u8>,
    pub sender_peer_id: String,
    pub timestamp: u64,
}

/// Sent as content_type 0x0051 (LIKE_BACK) — confirms a mutual match. Carries
/// the same conversation key re-encrypted to the original sender, so both sides
/// decrypt the same conversation key.
#[derive(uniffi::Record, serde::Serialize, serde::Deserialize, Clone)]
pub struct LikeBackPayload {
    pub conversation_id: String,
    pub conversation_key_enc: Vec<u8>, // encrypted back to original sender
    pub sender_peer_id: String,
    pub timestamp: u64,
}
