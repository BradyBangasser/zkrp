use crate::store::GhostStore;
use aes_gcm::aead::OsRng;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use std::sync::Arc;
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};
use zeroize::{Zeroize, ZeroizeOnDrop};

const NUM_ONE_TIME_PREKEYS: usize = 10;
const PREKEY_STORE_PREFIX: &str = "ghost/keybundle/prekeys/";
const SIGNED_PREKEY_KEY: &str = "ghost/keybundle/signed_prekey";
const IDENTITY_KEY: &str = "ghost/keybundle/identity_key";

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct KeyBundle {
    pub identity_key: [u8; 32],          // Ed25519 signing key bytes
    pub signed_prekey: [u8; 32],         // X25519 static secret bytes
    pub one_time_prekeys: Vec<[u8; 32]>, // X25519 one-time secrets
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PublicKeyBundle {
    pub identity_key: Vec<u8>,            // Ed25519 verifying key
    pub signed_prekey: Vec<u8>,           // X25519 public key
    pub signed_prekey_signature: Vec<u8>, // Ed25519 signature over signed_prekey
    pub one_time_prekeys: Vec<Vec<u8>>,   // X25519 public keys
}

impl KeyBundle {
    pub fn load_or_generate(store: &Arc<dyn GhostStore>) -> Self {
        if let Some(identity_bytes) = store.get(IDENTITY_KEY)
            && let Some(spk_bytes) = store.get(SIGNED_PREKEY_KEY)
            && identity_bytes.len() == 32
            && spk_bytes.len() == 32
        {
            let mut identity_key = [0u8; 32];
            let mut signed_prekey = [0u8; 32];
            identity_key.copy_from_slice(&identity_bytes);
            signed_prekey.copy_from_slice(&spk_bytes);

            let one_time_prekeys = store
                .list(PREKEY_STORE_PREFIX)
                .into_iter()
                .filter_map(|k| store.get(&k))
                .filter(|b| b.len() == 32)
                .map(|b| {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&b);
                    arr
                })
                .collect();

            tracing::info!("Loaded existing key bundle");
            return Self {
                identity_key,
                signed_prekey,
                one_time_prekeys,
            };
        }

        Self::generate(store)
    }

    fn generate(store: &Arc<dyn GhostStore>) -> Self {
        let identity_signing_key = SigningKey::generate(&mut OsRng);
        let identity_key = identity_signing_key.to_bytes();

        let signed_prekey_secret = StaticSecret::random_from_rng(OsRng);
        let signed_prekey = signed_prekey_secret.to_bytes();

        let one_time_prekeys: Vec<[u8; 32]> = (0..NUM_ONE_TIME_PREKEYS)
            .map(|_| StaticSecret::random_from_rng(OsRng).to_bytes())
            .collect();

        store.set(IDENTITY_KEY, identity_key.to_vec());
        store.set(SIGNED_PREKEY_KEY, signed_prekey.to_vec());
        for (i, otpk) in one_time_prekeys.iter().enumerate() {
            store.set(&format!("{}{}", PREKEY_STORE_PREFIX, i), otpk.to_vec());
        }

        tracing::info!(
            "Generated new key bundle with {} one-time prekeys",
            NUM_ONE_TIME_PREKEYS
        );
        Self {
            identity_key,
            signed_prekey,
            one_time_prekeys,
        }
    }

    pub fn public_bundle(&self) -> PublicKeyBundle {
        let signing_key = SigningKey::from_bytes(&self.identity_key);
        let identity_verifying = VerifyingKey::from(&signing_key);

        let spk_secret = StaticSecret::from(self.signed_prekey);
        let spk_public = X25519PublicKey::from(&spk_secret);

        let signature = signing_key.sign(spk_public.as_bytes());

        let one_time_public: Vec<Vec<u8>> = self
            .one_time_prekeys
            .iter()
            .map(|sk| {
                let secret = StaticSecret::from(*sk);
                X25519PublicKey::from(&secret).as_bytes().to_vec()
            })
            .collect();

        PublicKeyBundle {
            identity_key: identity_verifying.to_bytes().to_vec(),
            signed_prekey: spk_public.as_bytes().to_vec(),
            signed_prekey_signature: signature.to_bytes().to_vec(),
            one_time_prekeys: one_time_public,
        }
    }

    pub fn consume_one_time_prekey(
        &mut self,
        store: &Arc<dyn GhostStore>,
        index: usize,
    ) -> Option<[u8; 32]> {
        if index >= self.one_time_prekeys.len() {
            return None;
        }
        let key = self.one_time_prekeys.remove(index);
        store.delete(&format!("{}{}", PREKEY_STORE_PREFIX, index));
        // Replenish if running low
        if self.one_time_prekeys.len() < 3 {
            self.replenish_prekeys(store);
        }
        Some(key)
    }

    fn replenish_prekeys(&mut self, store: &Arc<dyn GhostStore>) {
        let start = self.one_time_prekeys.len();
        for i in start..(start + NUM_ONE_TIME_PREKEYS) {
            let key = StaticSecret::random_from_rng(OsRng).to_bytes();
            store.set(&format!("{}{}", PREKEY_STORE_PREFIX, i), key.to_vec());
            self.one_time_prekeys.push(key);
        }
        tracing::info!("Replenished {} one-time prekeys", NUM_ONE_TIME_PREKEYS);
    }
}
