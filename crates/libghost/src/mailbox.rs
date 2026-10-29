use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use zeroize::Zeroizing;

type HmacSha256 = Hmac<Sha256>;

const DAY: u64 = 86_400;
/// 16 bytes = 128-bit id. Unguessable, so knowing the id is itself the
/// capability to publish to it.
const ID_LEN: usize = 16;

/// Gossipsub topics that map 1:1 to a mailbox: `<app>/v1/mb/<id>`. The relay
/// routes/stores by this without learning more.
pub fn topic_for(app: &str, id: &str) -> String {
    format!("{app}/v1/mb/{id}")
}

/// Extract a mailbox id from a topic string, if it is one of `app`'s.
pub fn mailbox_id_from_topic<'a>(app: &str, topic: &'a str) -> Option<&'a str> {
    topic
        .strip_prefix(app)?
        .strip_prefix("/v1/mb/")
        .filter(|id| !id.is_empty())
}

/// A rotating mailbox derived from one shared secret and a purpose, within an
/// application's namespace (e.g. "fratrat"), so two apps on the same network
/// never derive the same ids.
pub struct RotatingMailbox {
    app: String,
    root: Zeroizing<[u8; 32]>,
    context: &'static str,
}

impl RotatingMailbox {
    /// Per-relationship message mailbox (your conversation after matching).
    /// `root` is the X25519 shared secret for this pair (see `keybundle`).
    pub fn dm(app: &str, root: [u8; 32]) -> Self {
        Self {
            app: app.to_string(),
            root: Zeroizing::new(root),
            context: "dm",
        }
    }

    /// Per-event mailbox shared by all attendees.
    pub fn party(app: &str, root: [u8; 32]) -> Self {
        Self {
            app: app.to_string(),
            root: Zeroizing::new(root),
            context: "party",
        }
    }

    /// Per-relationship rendezvous where the two of you exchange likes (see
    /// `crate::likes`). Separate namespace from `dm` so a like and a chat
    /// message never collide on the same topic.
    pub fn like(app: &str, root: [u8; 32]) -> Self {
        Self {
            app: app.to_string(),
            root: Zeroizing::new(root),
            context: "like",
        }
    }

    fn mac(&self) -> HmacSha256 {
        // HMAC accepts any key length; the fixed 32-byte root never fails.
        let mut m = HmacSha256::new_from_slice(self.root.as_ref()).expect("hmac key");
        m.update(format!("{}/mailbox/v1", self.app).as_bytes());
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
        self.active_window(now_unix).map(|id| topic_for(&self.app, &id))
    }
}

// --- store-and-forward client ------------------------------------------------
//
// Gossipsub only reaches peers online at that moment. A sender also deposits
// the (already end-to-end-encrypted) payload in the mailbox on a relay, and
// the recipient fetches what it missed on reconnect. The relay sees a mailbox
// id and ciphertext, and an anonymous admission; nothing else.

pub mod proto {
    tonic::include_proto!("zrp.mailbox.v1");
}

type ClientError = Box<dyn std::error::Error + Send + Sync>;
type Client = proto::mailbox_service_client::MailboxServiceClient<tonic::transport::Channel>;

/// One message a relay held for a mailbox. `msg_id` sorts chronologically and
/// is the cursor for the next fetch.
pub struct Held {
    pub msg_id: String,
    pub ciphertext: Vec<u8>,
}

async fn client(grpc_addr: &str) -> Result<Client, ClientError> {
    Ok(Client::new(crate::grpc::connect(grpc_addr).await?))
}

/// Leave `ciphertext` in `mailbox_id` on the relay at `grpc_addr`. The caller
/// encrypts; the relay stores whatever it is given. `sender_tag` is this
/// device's push tag for the mailbox (see `register`), so the relay doesn't
/// wake the sender; empty when it never registered.
pub async fn deposit(
    store: &dyn crate::store::GhostStore,
    grpc_addr: &str,
    mailbox_id: &str,
    ciphertext: Vec<u8>,
    sender_tag: Vec<u8>,
) -> Result<String, ClientError> {
    let mut request = tonic::Request::new(proto::DepositRequest {
        mailbox_id: mailbox_id.to_string(),
        ciphertext,
        sender_tag,
    });
    crate::admit::attach_grpc_admission(&mut request, store, grpc_addr, crate::admit::SERVICE_MAILBOX)
        .await;
    Ok(client(grpc_addr).await?.deposit(request).await?.into_inner().msg_id)
}

/// Ask to be woken when something is deposited in `mailbox_id`.
/// `sealed_token` is `zkcred::sealed_box::seal(push_public_key, token)`;
/// `tag` identifies this device within this mailbox only (16-32 bytes).
pub async fn register(
    store: &dyn crate::store::GhostStore,
    grpc_addr: &str,
    mailbox_id: &str,
    tag: Vec<u8>,
    sealed_token: Vec<u8>,
) -> Result<(), ClientError> {
    let mut request = tonic::Request::new(proto::RegisterRequest {
        mailbox_id: mailbox_id.to_string(),
        tag,
        sealed_token,
    });
    crate::admit::attach_grpc_admission(&mut request, store, grpc_addr, crate::admit::SERVICE_MAILBOX)
        .await;
    client(grpc_addr).await?.register(request).await?;
    Ok(())
}

/// Messages in `mailbox_id` after the cursor `after` (empty for all retained),
/// oldest first, and whether more remain.
pub async fn fetch(
    store: &dyn crate::store::GhostStore,
    grpc_addr: &str,
    mailbox_id: &str,
    after: &str,
) -> Result<(Vec<Held>, bool), ClientError> {
    let mut request = tonic::Request::new(proto::FetchRequest {
        mailbox_id: mailbox_id.to_string(),
        after: after.to_string(),
        limit: 0,
    });
    crate::admit::attach_grpc_admission(&mut request, store, grpc_addr, crate::admit::SERVICE_MAILBOX)
        .await;
    let resp = client(grpc_addr).await?.fetch(request).await?.into_inner();
    let held = resp
        .envelopes
        .into_iter()
        .map(|e| Held {
            msg_id: e.msg_id,
            ciphertext: e.ciphertext,
        })
        .collect();
    Ok((held, resp.has_more))
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
        let a = RotatingMailbox::dm("app", S);
        let b = RotatingMailbox::dm("app", S);
        assert_eq!(a.current_id(1_900_000_000), b.current_id(1_900_000_000));
    }

    #[test]
    fn rotates_daily_and_recovers_at_boundary() {
        let m = RotatingMailbox::dm("app", S);
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
            RotatingMailbox::dm("app", S).current_id(t),
            RotatingMailbox::party("app", S).current_id(t),
            "same secret, different purpose must not collide"
        );
        assert_ne!(
            RotatingMailbox::dm("app", S).current_id(t),
            RotatingMailbox::dm("app", [9u8; 32]).current_id(t),
            "different pair secrets must not collide"
        );
    }

    #[test]
    fn apps_do_not_collide_and_topics_round_trip() {
        let t = 1_900_000_000;
        assert_ne!(
            RotatingMailbox::dm("app", S).current_id(t),
            RotatingMailbox::dm("other", S).current_id(t)
        );
        let id = RotatingMailbox::dm("app", S).current_id(t);
        assert_eq!(mailbox_id_from_topic("app", &topic_for("app", &id)), Some(id.as_str()));
        assert_eq!(mailbox_id_from_topic("other", &topic_for("app", &id)), None);
    }

    #[test]
    fn id_shape() {
        let id = RotatingMailbox::dm("app", S).current_id(1_900_000_000);
        assert_eq!(id.len(), ID_LEN * 2);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
