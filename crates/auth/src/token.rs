//! The verification token: all enrollment state, carried by the client.
//!
//! The server stays stateless across the email round trip by sealing
//! {expiry, challenge, email} under a key only it holds. XChaCha20-Poly1305
//! rather than a bare MAC, so the email isn't readable in the magic link, the
//! app, or anything that logs URLs.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use zeroize::Zeroizing;

const VERSION: u8 = 1;
const AAD: &[u8] = b"zkrp/verify-token/v1";
const NONCE_LEN: usize = 24;
pub const CHALLENGE_LEN: usize = 32;

pub struct TokenKey(XChaCha20Poly1305);

pub struct Opened {
    pub challenge: [u8; CHALLENGE_LEN],
    pub email: Zeroizing<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("invalid token")]
    Invalid,
    #[error("token expired")]
    Expired,
}

impl TokenKey {
    pub fn new(key: &[u8]) -> Result<Self, String> {
        XChaCha20Poly1305::new_from_slice(key)
            .map(Self)
            .map_err(|_| format!("token key must be 32 bytes, got {}", key.len()))
    }

    pub fn seal(&self, email: &str, challenge: &[u8; CHALLENGE_LEN], expires_at: u64) -> Vec<u8> {
        let mut plain = Zeroizing::new(Vec::with_capacity(8 + CHALLENGE_LEN + email.len()));
        plain.extend_from_slice(&expires_at.to_be_bytes());
        plain.extend_from_slice(challenge);
        plain.extend_from_slice(email.as_bytes());

        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).expect("OS RNG");
        let ct = self
            .0
            .encrypt(XNonce::from_slice(&nonce), Payload { msg: &plain, aad: AAD })
            .expect("encryption into Vec cannot fail");

        let mut out = Vec::with_capacity(1 + NONCE_LEN + ct.len());
        out.push(VERSION);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        out
    }

    pub fn open(&self, token: &[u8], now: u64) -> Result<Opened, TokenError> {
        if token.len() < 1 + NONCE_LEN || token[0] != VERSION {
            return Err(TokenError::Invalid);
        }
        let (nonce, ct) = token[1..].split_at(NONCE_LEN);
        let plain = Zeroizing::new(
            self.0
                .decrypt(XNonce::from_slice(nonce), Payload { msg: ct, aad: AAD })
                .map_err(|_| TokenError::Invalid)?,
        );
        if plain.len() < 8 + CHALLENGE_LEN {
            return Err(TokenError::Invalid);
        }
        let expires_at = u64::from_be_bytes(plain[..8].try_into().unwrap());
        if now > expires_at {
            return Err(TokenError::Expired);
        }
        let challenge = plain[8..8 + CHALLENGE_LEN].try_into().unwrap();
        let email = std::str::from_utf8(&plain[8 + CHALLENGE_LEN..])
            .map_err(|_| TokenError::Invalid)?
            .to_string();
        Ok(Opened {
            challenge,
            email: Zeroizing::new(email),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_expiry() {
        let k = TokenKey::new(&[1u8; 32]).unwrap();
        let t = k.seal("a@iastate.edu", &[9u8; 32], 100);
        let o = k.open(&t, 100).unwrap();
        assert_eq!(&*o.email, "a@iastate.edu");
        assert_eq!(o.challenge, [9u8; 32]);
        assert!(matches!(k.open(&t, 101), Err(TokenError::Expired)));
    }

    #[test]
    fn rejects_tampering_and_other_keys() {
        let k = TokenKey::new(&[1u8; 32]).unwrap();
        let mut t = k.seal("a@iastate.edu", &[9u8; 32], 100);
        let other = TokenKey::new(&[2u8; 32]).unwrap();
        assert!(other.open(&t, 0).is_err());
        let last = t.len() - 1;
        t[last] ^= 1;
        assert!(k.open(&t, 0).is_err());
    }

    #[test]
    fn email_is_not_visible_in_token() {
        let k = TokenKey::new(&[1u8; 32]).unwrap();
        let t = k.seal("someone@iastate.edu", &[0u8; 32], 100);
        assert!(!t.windows(7).any(|w| w == b"someone"));
    }
}
