use crate::keybundle::KeyBundle;
use std::error::Error;

use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop, Debug)]
pub struct RatchetHeader {
    pub dh_ratchet_key: [u8; 32],
    pub pq_ratchet_key: Vec<u8>,
    pub message_index: u64,
    pub prev_chain_len: u64,
}

impl RatchetHeader {
    pub fn new() -> Self {
        Self {
            dh_ratchet_key: [0; 32],
            pq_ratchet_key: Vec::new(),
            message_index: 0,
            prev_chain_len: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop, Debug)]
pub struct X3DHInit {
    pub dh_identity_key: [u8; 32],
    pub pq_identity_key: Vec<u8>,
    pub ephemeral_key: [u8; 32],
    pub pq_ciphertext: Vec<u8>,
    pub one_time_prekey_id: u32,
    pub signature: Vec<u8>,
}

#[derive(Serialize, Deserialize, Zeroize, ZeroizeOnDrop, Debug)]
pub struct XRFMessage {
    pub r_header: RatchetHeader,
    pub payload: Vec<u8>,
    pub codec: u16,
    pub nonce: [u8; 12],
    pub x3dh_init: Option<X3DHInit>,
    pub signature: Vec<u8>,
}

impl XRFMessage {
    pub fn new(
        _kb: &KeyBundle,
        codec: u16,
        payload: &[u8],
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let secure_random = SystemRandom::new();
        let mut nonce = [0u8; 12];
        secure_random.fill(&mut nonce)?;
        Ok(Self {
            r_header: RatchetHeader::new(),
            payload: payload.to_vec(),
            codec,
            nonce,
            x3dh_init: None,
            signature: Vec::new(),
        })
    }
}
