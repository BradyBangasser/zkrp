//! Account identity on the mesh.
//!
//! A device has two keys. The *account* key is long-term: it is the identity
//! apps see (profiles, DM topics, encryption) and it signs every message. The
//! *transport* key is the libp2p identity, generated fresh on every launch, so
//! relays (which see transport peer ids and IPs, never message contents) can't
//! link one session to the next or to an account.
//!
//! Receivers attribute a message to the account that signed it, never to the
//! gossipsub peer that forwarded it.

use libp2p::PeerId;
use libp2p::identity::{Keypair, PublicKey};

use crate::protocols::ghost::v0::GhostMessage;

/// Every node subscribes; a signed hello is how peers learn which account sits
/// behind a new connection.
pub const HELLO_TOPIC: &str = "ghost/hello";
/// Messages signed further than this from now (either way) are dropped.
pub const MAX_SKEW_SECS: u64 = 600;
const DOMAIN: &[u8] = b"ghost/msg/v1";

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SignError {
    #[error("unsigned message")]
    Unsigned,
    #[error("bad sender key")]
    BadKey,
    #[error("bad signature")]
    BadSignature,
    #[error("message too old or from the future")]
    Stale,
}

fn signed_bytes(topic: &str, msg: &GhostMessage) -> Vec<u8> {
    fn field(out: &mut Vec<u8>, b: &[u8]) {
        out.extend_from_slice(&(b.len() as u32).to_be_bytes());
        out.extend_from_slice(b);
    }
    let mut out = DOMAIN.to_vec();
    field(&mut out, topic.as_bytes());
    out.extend_from_slice(&msg.codec.to_be_bytes());
    out.extend_from_slice(&msg.ts.to_be_bytes());
    out.extend_from_slice(&msg.nonce);
    field(&mut out, &msg.sender);
    field(&mut out, &msg.payload);
    out
}

/// Sign `msg` for `topic` with the account key.
pub fn sign(account: &Keypair, topic: &str, msg: &mut GhostMessage, now: u64) {
    msg.sender = account.public().encode_protobuf();
    msg.ts = now;
    msg.signature = account
        .sign(&signed_bytes(topic, msg))
        .expect("ed25519 signing is infallible");
}

/// The account that signed `msg` for `topic`.
pub fn verify(topic: &str, msg: &GhostMessage, now: u64) -> Result<PeerId, SignError> {
    if msg.signature.is_empty() || msg.sender.is_empty() {
        return Err(SignError::Unsigned);
    }
    if msg.ts.abs_diff(now) > MAX_SKEW_SECS {
        return Err(SignError::Stale);
    }
    let key = PublicKey::try_decode_protobuf(&msg.sender).map_err(|_| SignError::BadKey)?;
    if !key.verify(&signed_bytes(topic, msg), &msg.signature) {
        return Err(SignError::BadSignature);
    }
    Ok(key.to_peer_id())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(payload: &[u8]) -> GhostMessage {
        GhostMessage::new(0x0040, payload).unwrap()
    }

    #[test]
    fn attributes_to_the_signing_account() {
        let account = Keypair::generate_ed25519();
        let mut m = message(b"hi");
        sign(&account, "t", &mut m, 1000);
        assert_eq!(verify("t", &m, 1000), Ok(account.public().to_peer_id()));
    }

    #[test]
    fn rejects_tampering_topic_moves_and_staleness() {
        let account = Keypair::generate_ed25519();
        let mut m = message(b"hi");
        sign(&account, "t", &mut m, 1000);
        assert_eq!(verify("other", &m, 1000), Err(SignError::BadSignature));
        assert_eq!(verify("t", &m, 1000 + MAX_SKEW_SECS + 1), Err(SignError::Stale));
        m.payload = b"bye".to_vec();
        assert_eq!(verify("t", &m, 1000), Err(SignError::BadSignature));
    }

    #[test]
    fn rejects_impersonation() {
        let (alice, mallory) = (Keypair::generate_ed25519(), Keypair::generate_ed25519());
        let mut m = message(b"hi");
        sign(&mallory, "t", &mut m, 1000);
        m.sender = alice.public().encode_protobuf();
        assert_eq!(verify("t", &m, 1000), Err(SignError::BadSignature));
        assert_eq!(verify("t", &message(b"x"), 1000), Err(SignError::Unsigned));
    }
}
