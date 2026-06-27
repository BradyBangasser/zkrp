use crate::store::GhostStore;
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use libp2p::identity::PublicKey;
use libp2p::{PeerId, identity};
use sha2::{Digest, Sha512};
use std::sync::Arc;
use x25519_dalek::{EphemeralSecret, PublicKey as X25519PublicKey, StaticSecret};

pub struct NodeIdentity {
    pub keypair: identity::Keypair,
    pub peer_id: PeerId,
}

impl NodeIdentity {
    pub fn generate() -> Self {
        let keypair = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(keypair.public());
        Self { keypair, peer_id }
    }

    pub fn peer_id_string(&self) -> String {
        self.peer_id.to_string()
    }

    pub fn to_keypair_bytes(&self) -> Vec<u8> {
        self.keypair
            .to_protobuf_encoding()
            .expect("Ed25519 keypair serialization is infallible")
    }

    pub fn from_keypair_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let keypair = identity::Keypair::from_protobuf_encoding(bytes)?;
        let peer_id = PeerId::from(keypair.public());
        Ok(Self { keypair, peer_id })
    }

    pub fn load_or_generate(store: &Arc<dyn GhostStore>) -> Self {
        if let Some(bytes) = store.get("ghost/identity/keypair")
            && let Ok(keypair) = libp2p::identity::Keypair::from_protobuf_encoding(&bytes)
        {
            let peer_id = keypair.public().to_peer_id();
            tracing::info!("Loaded stable identity: {}", peer_id);
            return Self { keypair, peer_id };
        }
        let identity = Self::generate();
        let bytes = identity
            .keypair
            .to_protobuf_encoding()
            .expect("failed to encode keypair");
        store.set("ghost/identity/keypair", bytes);
        store.set("ghost/identity/peer_id", identity.peer_id.to_bytes());
        tracing::info!("Generated new identity: {}", identity.peer_id.to_string());
        identity
    }

    fn to_x25519_secret(&self) -> StaticSecret {
        let ed_bytes = self
            .keypair
            .to_protobuf_encoding()
            .expect("keypair encoding infallible");
        let seed = &ed_bytes[ed_bytes.len() - 64..ed_bytes.len() - 32];
        let mut hash = Sha512::digest(seed);
        hash[0] &= 248;
        hash[31] &= 127;
        hash[31] |= 64;
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&hash[..32]);
        StaticSecret::from(key_bytes)
    }

    fn x25519_pubkey_for_peer(
        peer_id_str: &str,
    ) -> Result<X25519PublicKey, Box<dyn std::error::Error>> {
        let peer_id: libp2p::PeerId = peer_id_str.parse()?;
        let pub_key = PublicKey::try_decode_protobuf(peer_id.to_bytes().as_slice())?;
        let ed_pub = pub_key.try_into_ed25519()?;
        let ed_bytes = ed_pub.to_bytes();
        let x25519_bytes = ed25519_to_x25519_pubkey(&ed_bytes);
        Ok(X25519PublicKey::from(x25519_bytes))
    }

    pub fn generate_conversation_key() -> Vec<u8> {
        use ring::rand::{SecureRandom, SystemRandom};
        let rng = SystemRandom::new();
        let mut key = vec![0u8; 32];
        rng.fill(&mut key)
            .expect("SystemRandom::fill failed — OS entropy unavailable");
        key
    }

    pub fn encrypt_for_peer(
        &self,
        plaintext: &[u8],
        peer_id_str: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let recipient_x25519 = Self::x25519_pubkey_for_peer(peer_id_str)?;

        let ephemeral_secret = EphemeralSecret::random_from_rng(OsRng);
        let ephemeral_public = X25519PublicKey::from(&ephemeral_secret);

        let shared = ephemeral_secret.diffie_hellman(&recipient_x25519);

        let enc_key = hkdf_sha256(shared.as_bytes(), b"fratrat-msg-enc");
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&enc_key));
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);

        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| format!("ChaCha20Poly1305 encrypt failed: {e}"))?;

        let mut out = Vec::with_capacity(32 + 12 + ciphertext.len());
        out.extend_from_slice(ephemeral_public.as_bytes());
        out.extend_from_slice(nonce.as_slice());
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        if ciphertext.len() < 32 + 12 + 16 {
            return Err(format!(
                "ciphertext too short: {} bytes (minimum 60)",
                ciphertext.len()
            )
            .into());
        }

        let ephemeral_pub_bytes: [u8; 32] = ciphertext[..32]
            .try_into()
            .expect("slice length checked above");
        let nonce_bytes: [u8; 12] = ciphertext[32..44]
            .try_into()
            .expect("slice length checked above");
        let ct = &ciphertext[44..];

        let ephemeral_pub = X25519PublicKey::from(ephemeral_pub_bytes);

        let our_secret = self.to_x25519_secret();

        let shared = our_secret.diffie_hellman(&ephemeral_pub);
        let enc_key = hkdf_sha256(shared.as_bytes(), b"fratrat-msg-enc");

        let cipher = ChaCha20Poly1305::new(Key::from_slice(&enc_key));
        let nonce = Nonce::from_slice(&nonce_bytes);

        cipher
            .decrypt(nonce, ct)
            .map_err(|e| format!("ChaCha20Poly1305 decrypt failed: {e}").into())
    }
}

fn ed25519_to_x25519_pubkey(ed_bytes: &[u8; 32]) -> [u8; 32] {
    use curve25519_dalek::edwards::CompressedEdwardsY;
    let compressed = CompressedEdwardsY(*ed_bytes);
    if let Some(point) = compressed.decompress() {
        point.to_montgomery().to_bytes()
    } else {
        tracing::error!("ed25519_to_x25519_pubkey: failed to decompress Edwards point");
        [0u8; 32]
    }
}

fn hkdf_sha256(ikm: &[u8], info: &[u8]) -> [u8; 32] {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let salt = [0u8; 32];
    let mut mac = HmacSha256::new_from_slice(&salt).expect("HMAC accepts any key length");
    mac.update(ikm);
    let prk = mac.finalize().into_bytes();

    let mut mac = HmacSha256::new_from_slice(&prk).expect("HMAC accepts any key length");
    mac.update(info);
    mac.update(&[0x01]);
    let okm = mac.finalize().into_bytes();

    let mut out = [0u8; 32];
    out.copy_from_slice(&okm);
    out
}
