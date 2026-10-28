//! Sealed topics.
//!
//! A signed message names its author's account (`crate::signed`), so on an
//! open topic every relay that forwards it learns which account is behind the
//! connection, and the fresh transport key stops protecting anything. On a
//! sealed topic the whole signed message (author, signature, content type and
//! payload) is encrypted under a key the app registered for that topic, and
//! travels inside an anonymous outer message with no author, signature or
//! timestamp. Relays see the topic name and a ciphertext; members open it and
//! verify the signature inside as usual.
//!
//! A sealed topic accepts only sealed messages. In sealed-only mode
//! (`Seals::require`) nothing is published in the clear at all: a publish to
//! a topic with no key is dropped, and the hello that introduces an account to
//! its peers goes only to the app's chosen sealed topics.
//!
//! Lengths are not hidden.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit as _, OsRng, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use zeroize::Zeroizing;

const NONCE_LEN: usize = 12;
const DOMAIN: &[u8] = b"zkrp/sealed/v1";

/// The encryption key for a topic key, so an app can use the same key for
/// other derivations (mailbox ids, its own payload sealing) without overlap.
fn enc_key(key: &[u8; 32]) -> Zeroizing<[u8; 32]> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac key");
    m.update(DOMAIN);
    Zeroizing::new(m.finalize().into_bytes().into())
}

fn aad(topic: &str) -> Vec<u8> {
    let mut out = DOMAIN.to_vec();
    out.extend_from_slice(topic.as_bytes());
    out
}

/// Encrypt `plaintext` for `topic`: `nonce(12) || ciphertext`. The topic is
/// bound in, so a sealed message can't be replayed onto another topic that
/// happens to share the key.
pub fn seal(key: &[u8; 32], topic: &str, plaintext: &[u8]) -> Vec<u8> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(enc_key(key).as_ref()));
    let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: &aad(topic),
            },
        )
        .expect("chacha20poly1305 encrypt is infallible for in-memory buffers");
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(nonce.as_slice());
    out.extend_from_slice(&ciphertext);
    out
}

/// Decrypt what `seal` produced for `topic`, or None.
pub fn open(key: &[u8; 32], topic: &str, sealed: &[u8]) -> Option<Vec<u8>> {
    if sealed.len() < NONCE_LEN + 16 {
        return None;
    }
    let (nonce, ciphertext) = sealed.split_at(NONCE_LEN);
    ChaCha20Poly1305::new(Key::from_slice(enc_key(key).as_ref()))
        .decrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: &aad(topic),
            },
        )
        .ok()
}

/// The sealed topics of one running context, shared by its handle and the
/// task that signs, seals and opens messages.
#[derive(Default)]
pub struct Seals {
    keys: RwLock<HashMap<String, Zeroizing<[u8; 32]>>>,
    required: AtomicBool,
    hello_topics: RwLock<Vec<String>>,
}

impl Seals {
    /// Seal `topic` with `key` from now on (replacing any earlier key).
    pub fn insert(&self, topic: String, key: [u8; 32]) {
        self.keys.write().unwrap().insert(topic, Zeroizing::new(key));
    }

    pub fn remove(&self, topic: &str) {
        self.keys.write().unwrap().remove(topic);
    }

    pub fn key(&self, topic: &str) -> Option<Zeroizing<[u8; 32]>> {
        self.keys.read().unwrap().get(topic).cloned()
    }

    /// Sealed-only mode: never publish in the clear.
    pub fn require(&self, required: bool) {
        self.required.store(required, Ordering::Relaxed);
    }

    pub fn required(&self) -> bool {
        self.required.load(Ordering::Relaxed)
    }

    /// Where the hello goes in sealed-only mode (normally the app's sealed
    /// topics that every peer it wants to meet also watches).
    pub fn set_hello_topics(&self, topics: Vec<String>) {
        *self.hello_topics.write().unwrap() = topics;
    }

    /// The topics to send a hello on right now.
    pub fn hello_topics(&self) -> Vec<String> {
        if self.required() {
            self.hello_topics.read().unwrap().clone()
        } else {
            vec![crate::signed::HELLO_TOPIC.to_string()]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: [u8; 32] = [3u8; 32];

    #[test]
    fn round_trips_and_hides_the_plaintext() {
        let sealed = seal(&K, "t", b"account key and signature");
        assert!(!sealed.windows(7).any(|w| w == b"account"));
        assert_eq!(open(&K, "t", &sealed).unwrap(), b"account key and signature");
    }

    #[test]
    fn wrong_key_or_topic_fails() {
        let sealed = seal(&K, "t", b"hi");
        assert!(open(&[4u8; 32], "t", &sealed).is_none());
        assert!(open(&K, "other", &sealed).is_none());
        assert!(open(&K, "t", &sealed[..10]).is_none());
    }

    #[test]
    fn hello_goes_only_to_chosen_topics_when_sealed_only() {
        let s = Seals::default();
        assert_eq!(s.hello_topics(), [crate::signed::HELLO_TOPIC]);
        s.require(true);
        assert!(s.hello_topics().is_empty());
        s.set_hello_topics(vec!["campus".into()]);
        assert_eq!(s.hello_topics(), ["campus"]);
    }
}
