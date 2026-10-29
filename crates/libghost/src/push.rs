//! Push registration, client side (proto/mailbox.proto, proto/push.proto).
//!
//! A device registers its push token under each mailbox it watches, sealed to
//! the network's push service (`zkcred::sealed_box`), with a tag that names
//! this device within that mailbox only. Relays store boxes they can't open
//! or link; when something is deposited they hand the push service every box
//! but the sender's (the sender passes its own tag to `mailbox::deposit`).

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::store::GhostStore;

/// Which push service a token belongs to (the first byte sealed with it).
pub const APNS_PRODUCTION: u8 = 1;
pub const APNS_SANDBOX: u8 = 2;

const SECRET_KEY: &str = "zkrp/push/secret";
const REGISTERED_PREFIX: &str = "zkrp/push/registered/";
const TAG_LEN: usize = 16;

/// A random secret that never leaves the device; tags derive from it.
fn device_secret(store: &dyn GhostStore) -> Vec<u8> {
    if let Some(s) = store.get(SECRET_KEY).filter(|s| s.len() == 32) {
        return s;
    }
    let s = crate::identity::NodeIdentity::generate_conversation_key();
    store.set(SECRET_KEY, s.clone());
    s
}

fn mac(store: &dyn GhostStore, label: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(&device_secret(store)).expect("hmac key");
    m.update(label);
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

/// This device's tag in `mailbox_id`: stable for the mailbox, unrelated to
/// its tag in any other.
pub fn tag(store: &dyn GhostStore, mailbox_id: &str) -> Vec<u8> {
    mac(store, b"zkrp/push/tag/v1", mailbox_id.as_bytes())[..TAG_LEN].to_vec()
}

/// Register `token` (for `service`) under `mailbox_id` with the relay at
/// `grpc_addr`, sealed to `push_public_key`. Skips the call when this mailbox
/// already has this token; returns whether it registered.
pub async fn register(
    store: &dyn GhostStore,
    grpc_addr: &str,
    push_public_key: &[u8],
    mailbox_id: &str,
    service: u8,
    token: &[u8],
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let public: [u8; 32] = push_public_key.try_into().map_err(|_| "no push key")?;
    let mut plain = vec![service];
    plain.extend_from_slice(token);
    let done_key = format!("{REGISTERED_PREFIX}{mailbox_id}");
    let fingerprint = mac(store, b"zkrp/push/registered/v1", &plain)[..8].to_vec();
    if store.get(&done_key).as_deref() == Some(fingerprint.as_slice()) {
        return Ok(false);
    }
    let sealed = zkcred::sealed_box::seal(&public, &plain);
    crate::mailbox::register(store, grpc_addr, mailbox_id, tag(store, mailbox_id), sealed).await?;
    store.set(&done_key, fingerprint);
    Ok(true)
}

/// Forget which mailboxes were registered (e.g. after the token changed
/// service or the push key rotated), so the next `register` calls go out.
pub fn forget_registrations(store: &dyn GhostStore) {
    for k in store.list(REGISTERED_PREFIX) {
        store.delete(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[test]
    fn tags_are_stable_per_mailbox_and_differ_across() {
        let store = MemoryStore::new();
        let a = tag(&store, "aaaa");
        assert_eq!(a, tag(&store, "aaaa"));
        assert_ne!(a, tag(&store, "bbbb"));
        assert_eq!(a.len(), TAG_LEN);
        assert_ne!(a, tag(&MemoryStore::new(), "aaaa"), "per device");
    }
}
