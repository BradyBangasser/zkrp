//! Anonymous sealed boxes: encrypt to a long-term X25519 public key with a
//! fresh ephemeral key each time, so two boxes of the same plaintext share
//! nothing. Used for push tokens: the app seals its device token to the push
//! service, and relays store and forward boxes they can neither open nor
//! link to each other.
//!
//! Format: `ephemeral_public(32) || nonce(12) || ciphertext`.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hmac::{Hmac, Mac};
use rand::RngCore;
use x25519_dalek::{EphemeralSecret, PublicKey, StaticSecret};

const DOMAIN: &[u8] = b"zkrp/sealed-box/v1";

fn key(shared: &[u8; 32], ephemeral: &[u8; 32], recipient: &[u8; 32]) -> [u8; 32] {
    let mut m = <Hmac<sha2::Sha256> as Mac>::new_from_slice(shared).expect("hmac key");
    m.update(DOMAIN);
    m.update(ephemeral);
    m.update(recipient);
    m.finalize().into_bytes().into()
}

/// A new recipient key pair: (secret, public).
pub fn keypair() -> ([u8; 32], [u8; 32]) {
    let secret = StaticSecret::random_from_rng(rand::rngs::OsRng);
    let public = PublicKey::from(&secret);
    (secret.to_bytes(), public.to_bytes())
}

pub fn public_key(secret: &[u8; 32]) -> [u8; 32] {
    PublicKey::from(&StaticSecret::from(*secret)).to_bytes()
}

/// Seal `plaintext` so only the holder of `recipient`'s secret can open it.
pub fn seal(recipient: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
    let ephemeral = EphemeralSecret::random_from_rng(rand::rngs::OsRng);
    let ephemeral_public = PublicKey::from(&ephemeral).to_bytes();
    let shared = ephemeral.diffie_hellman(&PublicKey::from(*recipient));
    let k = key(shared.as_bytes(), &ephemeral_public, recipient);
    let mut nonce = [0u8; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let ciphertext = ChaCha20Poly1305::new(Key::from_slice(&k))
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: plaintext, aad: DOMAIN })
        .expect("in-memory encrypt");
    let mut out = Vec::with_capacity(32 + 12 + ciphertext.len());
    out.extend_from_slice(&ephemeral_public);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    out
}

/// Open a box sealed to the public key of `secret`.
pub fn open(secret: &[u8; 32], sealed: &[u8]) -> Option<Vec<u8>> {
    if sealed.len() < 32 + 12 + 16 {
        return None;
    }
    let ephemeral_public: [u8; 32] = sealed[..32].try_into().ok()?;
    let secret = StaticSecret::from(*secret);
    let recipient = PublicKey::from(&secret).to_bytes();
    let shared = secret.diffie_hellman(&PublicKey::from(ephemeral_public));
    let k = key(shared.as_bytes(), &ephemeral_public, &recipient);
    ChaCha20Poly1305::new(Key::from_slice(&k))
        .decrypt(
            Nonce::from_slice(&sealed[32..44]),
            Payload { msg: &sealed[44..], aad: DOMAIN },
        )
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_boxes_are_unlinkable() {
        let (secret, public) = keypair();
        assert_eq!(public_key(&secret), public);
        let a = seal(&public, b"device token");
        let b = seal(&public, b"device token");
        assert_ne!(a, b);
        assert_ne!(a[..32], b[..32], "fresh ephemeral key each time");
        assert_eq!(open(&secret, &a).unwrap(), b"device token");
        assert_eq!(open(&secret, &b).unwrap(), b"device token");
    }

    #[test]
    fn only_the_recipient_opens_it() {
        let (secret, public) = keypair();
        let (other, _) = keypair();
        let sealed = seal(&public, b"t");
        assert!(open(&other, &sealed).is_none());
        let mut tampered = sealed.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(open(&secret, &tampered).is_none());
        assert!(open(&secret, &sealed[..40]).is_none());
    }
}
