pub mod v0 {
    use std::time::{SystemTime, UNIX_EPOCH};

    include!(concat!(env!("OUT_DIR"), "/zrp.v0.ghost.rs"));

    pub fn encode(payload: &[u8]) -> GhostEnvelope {
        GhostEnvelope {
            version: 0,
            time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            payload: payload.to_vec(),
        }
    }
}
