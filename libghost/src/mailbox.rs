use aws_sdk_s3::{self as s3, primitives::ByteStream};
use hmac::{Hmac, KeyInit, Mac};
use ring::rand::SecureRandom;
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use zeroize::Zeroizing;

/// Reject oversized deposits early. A like or chat message is tiny; this is a
/// guard against a peer who knows a mailbox id trying to fill the bucket.
pub const MAX_DEPOSIT_BYTES: usize = 256 * 1024;
pub const DEFAULT_FETCH_LIMIT: i32 = 100;
pub const MAX_FETCH_LIMIT: i32 = 500;

const PREFIX: &str = "mb/";

type HmacSha256 = Hmac<Sha256>;

const DAY: u64 = 86_400;
const DOMAIN: &[u8] = b"fratrat/mailbox/v1";
/// 16 bytes = 128-bit id. Unguessable, so knowing the id is itself the
/// capability to publish to it.
const ID_LEN: usize = 16;

/// Gossipsub topics and blob namespaces that map 1:1 to a mailbox:
/// `fratrat/v1/mb/<id>`. The relay routes/stores by this without learning more.
pub const TOPIC_PREFIX: &str = "fratrat/v1/mb/";

pub fn topic_for(id: &str) -> String {
    format!("{TOPIC_PREFIX}{id}")
}

/// Extract a mailbox id from a topic string, if it is one.
pub fn mailbox_id_from_topic(topic: &str) -> Option<&str> {
    topic.strip_prefix(TOPIC_PREFIX).filter(|id| !id.is_empty())
}

/// A rotating mailbox derived from one shared secret and a purpose.
pub struct RotatingMailbox {
    root: Zeroizing<[u8; 32]>,
    context: &'static str,
}

impl RotatingMailbox {
    /// Per-relationship message mailbox (your conversation after matching).
    /// `root` is the X25519 shared secret for this pair (see `keybundle`).
    pub fn dm(root: [u8; 32]) -> Self {
        Self {
            root: Zeroizing::new(root),
            context: "dm",
        }
    }

    /// Per-event mailbox shared by all attendees.
    pub fn party(root: [u8; 32]) -> Self {
        Self {
            root: Zeroizing::new(root),
            context: "party",
        }
    }

    /// Per-relationship rendezvous where the two of you exchange likes (see
    /// `crate::likes`). Separate namespace from `dm` so a like and a chat
    /// message never collide on the same topic.
    pub fn like(root: [u8; 32]) -> Self {
        Self {
            root: Zeroizing::new(root),
            context: "like",
        }
    }

    fn mac(&self) -> HmacSha256 {
        // HMAC accepts any key length; the fixed 32-byte root never fails.
        let mut m = HmacSha256::new_from_slice(self.root.as_ref()).expect("hmac key");
        m.update(DOMAIN);
        m.update(&[self.context.len() as u8]);
        m.update(self.context.as_bytes());
        m
    }

    /// Per-secret rotation offset in seconds, so not everything flips at 00:00.
    fn offset(&self) -> u64 {
        let mut m = self.mac();
        m.update(b"offset");
        let out = m.finalize().into_bytes();
        u64::from_be_bytes(out[..8].try_into().unwrap()) % DAY
    }

    /// The 24h epoch index for a unix time, shifted by this secret's offset.
    pub fn epoch_day(&self, now_unix: u64) -> u64 {
        now_unix.saturating_sub(self.offset()) / DAY
    }

    /// The mailbox id for a specific epoch day.
    pub fn id_for_day(&self, epoch_day: u64) -> String {
        let mut m = self.mac();
        m.update(b"id");
        m.update(&epoch_day.to_be_bytes());
        let out = m.finalize().into_bytes();
        hex(&out[..ID_LEN])
    }

    /// Today's mailbox id — where a sender publishes.
    pub fn current_id(&self, now_unix: u64) -> String {
        self.id_for_day(self.epoch_day(now_unix))
    }

    /// {previous, current, next} ids — what a recipient watches so nothing is
    /// missed across the rotation boundary.
    pub fn active_window(&self, now_unix: u64) -> [String; 3] {
        let d = self.epoch_day(now_unix);
        [
            self.id_for_day(d.wrapping_sub(1)),
            self.id_for_day(d),
            self.id_for_day(d + 1),
        ]
    }

    /// The same window as gossipsub topics.
    pub fn active_topics(&self, now_unix: u64) -> [String; 3] {
        self.active_window(now_unix).map(|id| topic_for(&id))
    }
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: [u8; 32] = [7u8; 32];

    #[test]
    fn both_sides_agree() {
        let a = RotatingMailbox::dm(S);
        let b = RotatingMailbox::dm(S);
        assert_eq!(a.current_id(1_900_000_000), b.current_id(1_900_000_000));
    }

    #[test]
    fn rotates_daily_and_recovers_at_boundary() {
        let m = RotatingMailbox::dm(S);
        let t = 1_900_000_000;
        let today = m.current_id(t);
        let tomorrow = m.current_id(t + DAY);
        assert_ne!(today, tomorrow);
        // Tomorrow's watcher still covers today via the window (prev).
        assert!(m.active_window(t + DAY).contains(&today));
    }

    #[test]
    fn context_and_secret_separate_the_namespaces() {
        let t = 1_900_000_000;
        assert_ne!(
            RotatingMailbox::dm(S).current_id(t),
            RotatingMailbox::party(S).current_id(t),
            "same secret, different purpose must not collide"
        );
        assert_ne!(
            RotatingMailbox::dm(S).current_id(t),
            RotatingMailbox::dm([9u8; 32]).current_id(t),
            "different pair secrets must not collide"
        );
    }

    #[test]
    fn id_shape() {
        let id = RotatingMailbox::dm(S).current_id(1_900_000_000);
        assert_eq!(id.len(), ID_LEN * 2);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
#[derive(Clone)]
pub struct MailboxStore {
    client: s3::Client,
    bucket: String,
}

pub struct StoredEnvelope {
    pub msg_id: String,
    pub ciphertext: Vec<u8>,
    pub stored_at_millis: u64,
}

impl MailboxStore {
    pub async fn new(bucket: String) -> Self {
        let cfg = aws_config::load_from_env().await;
        Self {
            client: s3::Client::new(&cfg),
            bucket,
        }
    }

    /// Mailbox ids are a capability, but they still flow into an S3 key, so keep
    /// them to an unambiguous, path-safe alphabet.
    fn valid_id(id: &str) -> bool {
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    }

    fn key(mailbox_id: &str, msg_id: &str) -> String {
        format!("{PREFIX}{mailbox_id}/{msg_id}")
    }

    /// Store one ciphertext blob, returning its sortable msg_id.
    pub async fn deposit(
        &self,
        mailbox_id: &str,
        ciphertext: Vec<u8>,
    ) -> Result<String, StoreError> {
        if !Self::valid_id(mailbox_id) {
            return Err(StoreError::BadId);
        }
        if ciphertext.is_empty() {
            return Err(StoreError::Empty);
        }
        if ciphertext.len() > MAX_DEPOSIT_BYTES {
            return Err(StoreError::TooLarge);
        }

        let msg_id = new_msg_id();
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(Self::key(mailbox_id, &msg_id))
            .body(ByteStream::from(ciphertext))
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| StoreError::S3(e.to_string()))?;
        Ok(msg_id)
    }

    /// List envelopes after `cursor` (exclusive), oldest first, up to `limit`.
    /// Returns (envelopes, has_more).
    pub async fn fetch(
        &self,
        mailbox_id: &str,
        after: &str,
        limit: i32,
    ) -> Result<(Vec<StoredEnvelope>, bool), StoreError> {
        if !Self::valid_id(mailbox_id) {
            return Err(StoreError::BadId);
        }
        let limit = if limit <= 0 {
            DEFAULT_FETCH_LIMIT
        } else {
            limit.min(MAX_FETCH_LIMIT)
        };

        let mb_prefix = format!("{PREFIX}{mailbox_id}/");
        // start-after wants a full key; build one from the cursor msg_id.
        let start_after = if after.is_empty() {
            None
        } else {
            Some(Self::key(mailbox_id, after))
        };

        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&mb_prefix)
            .max_keys(limit);
        if let Some(sa) = start_after {
            req = req.start_after(sa);
        }
        let listing = req
            .send()
            .await
            .map_err(|e| StoreError::S3(e.to_string()))?;

        let has_more = listing.is_truncated().unwrap_or(false);
        let keys: Vec<String> = listing
            .contents()
            .iter()
            .filter_map(|o| o.key().map(str::to_string))
            .collect();

        // Fetch bodies in listed (chronological) order.
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(msg_id) = key.rsplit('/').next().map(str::to_string) else {
                continue;
            };
            let obj = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| StoreError::S3(e.to_string()))?;
            let bytes = obj
                .body
                .collect()
                .await
                .map_err(|e| StoreError::S3(e.to_string()))?
                .into_bytes();
            let stored_at_millis = msg_id
                .split('-')
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            out.push(StoredEnvelope {
                msg_id,
                ciphertext: bytes.to_vec(),
                stored_at_millis,
            });
        }
        Ok((out, has_more))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("bad mailbox id")]
    BadId,
    #[error("empty ciphertext")]
    Empty,
    #[error("ciphertext too large")]
    TooLarge,
    #[error("s3: {0}")]
    S3(String),
}

/// `<zero-padded unix millis>-<8 hex>` — sorts lexicographically by time.
fn new_msg_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut rnd = [0u8; 4];
    let _ = ring::rand::SystemRandom::new().fill(&mut rnd);
    format!(
        "{millis:013}-{:02x}{:02x}{:02x}{:02x}",
        rnd[0], rnd[1], rnd[2], rnd[3]
    )
}
