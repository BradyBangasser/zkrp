//! S3 store-and-forward for mailboxes. Server-side only: clients derive
//! mailbox ids with `libghost::mailbox::RotatingMailbox` and never touch S3.

use aws_sdk_s3::{self as s3, primitives::ByteStream};
use ring::rand::SecureRandom;
use std::time::{SystemTime, UNIX_EPOCH};

/// Reject oversized deposits early. A like or chat message is tiny; this is a
/// guard against a peer who knows a mailbox id trying to fill the bucket.
pub const MAX_DEPOSIT_BYTES: usize = 256 * 1024;
pub const DEFAULT_FETCH_LIMIT: i32 = 100;
pub const MAX_FETCH_LIMIT: i32 = 500;

const PREFIX: &str = "mb/";
/// Push registrations: `push/<mailbox id>/<hex tag>` holding a sealed token.
const PUSH_PREFIX: &str = "push/";
/// A sealed APNs/FCM token is well under this.
pub const MAX_SEALED_TOKEN_BYTES: usize = 512;
/// Registrations per mailbox handed to the push service, at most.
const MAX_REGISTRATIONS: i32 = 16;

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
    pub fn new(client: s3::Client, bucket: String) -> Self {
        Self { client, bucket }
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

impl MailboxStore {
    /// Store (or replace) one device's sealed push token for a mailbox.
    pub async fn register(
        &self,
        mailbox_id: &str,
        tag: &[u8],
        sealed_token: Vec<u8>,
    ) -> Result<(), StoreError> {
        if !Self::valid_id(mailbox_id) || !(16..=32).contains(&tag.len()) {
            return Err(StoreError::BadId);
        }
        if sealed_token.is_empty() {
            return Err(StoreError::Empty);
        }
        if sealed_token.len() > MAX_SEALED_TOKEN_BYTES {
            return Err(StoreError::TooLarge);
        }
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(format!("{PUSH_PREFIX}{mailbox_id}/{}", hex::encode(tag)))
            .body(ByteStream::from(sealed_token))
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| StoreError::S3(e.to_string()))?;
        Ok(())
    }

    /// The sealed tokens registered for a mailbox, minus the one under
    /// `except_tag` (the sender's own device).
    pub async fn sealed_tokens(
        &self,
        mailbox_id: &str,
        except_tag: &[u8],
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        if !Self::valid_id(mailbox_id) {
            return Err(StoreError::BadId);
        }
        let prefix = format!("{PUSH_PREFIX}{mailbox_id}/");
        let skip = format!("{prefix}{}", hex::encode(except_tag));
        let listing = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .max_keys(MAX_REGISTRATIONS)
            .send()
            .await
            .map_err(|e| StoreError::S3(e.to_string()))?;
        let mut out = Vec::new();
        for key in listing.contents().iter().filter_map(|o| o.key()) {
            if key == skip {
                continue;
            }
            let obj = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| StoreError::S3(e.to_string()))?;
            let bytes = obj
                .body
                .collect()
                .await
                .map_err(|e| StoreError::S3(e.to_string()))?
                .into_bytes();
            out.push(bytes.to_vec());
        }
        Ok(out)
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
