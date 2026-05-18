pub mod v0 {
    include!(concat!(env!("OUT_DIR"), "/zrp.v0.ghost.rs"));

    pub fn encode(payload: &[u8]) -> GhostEnvelope {
        GhostEnvelope {
            version: 0,
            time_nonce: rand::random(),
            payload: payload.to_vec(),
        }
    }

    mod header;
    mod message;
}
