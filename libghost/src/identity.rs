use libp2p::{PeerId, identity};

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
}
