use crate::store::GhostStore;
use libp2p::{PeerId, identity};
use std::sync::Arc;

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
}
