use libp2p::{
    PeerId, autonat, dcutr, identify,
    kad::{self, store::MemoryStore},
    mdns, relay,
    swarm::NetworkBehaviour,
};

#[derive(NetworkBehaviour)]
pub struct MeshBehavior {
    pub relay_client: relay::client::Behaviour,
    pub relay_server: relay::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub autonat: autonat::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub mdns: mdns::tokio::Behaviour,
}

impl MeshBehavior {
    pub fn new(
        local_public_key: libp2p::identity::PublicKey,
        relay_client: relay::client::Behaviour,
    ) -> Self {
        let local_peer_id = PeerId::from(local_public_key.clone());

        Self {
            relay_client,
            relay_server: relay::Behaviour::new(local_peer_id, relay::Config::default()),
            dcutr: dcutr::Behaviour::new(local_peer_id),
            autonat: autonat::Behaviour::new(local_peer_id, autonat::Config::default()),
            identify: identify::Behaviour::new(identify::Config::new(
                "/nocap/1.0.0".into(),
                local_public_key,
            )),
            kademlia: kad::Behaviour::new(local_peer_id, MemoryStore::new(local_peer_id)),
            mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
                .expect("mDNS setup failed"),
        }
    }
}
