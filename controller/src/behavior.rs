use libp2p::{
    PeerId, autonat, dcutr, gossipsub, identify,
    identity::Keypair,
    kad::{self, store::MemoryStore},
    mdns, noise, relay,
    swarm::NetworkBehaviour,
};

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    time::Duration,
};

#[derive(NetworkBehaviour)]
pub struct MeshBehavior {
    pub relay_client: relay::client::Behaviour,
    pub relay_server: relay::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub autonat: autonat::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

impl MeshBehavior {
    pub fn new(keypair: &Keypair, relay_client: relay::client::Behaviour) -> Self {
        let local_peer_id = PeerId::from(keypair.public().clone());

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .mesh_n(2)
            .mesh_n_low(1)
            .mesh_n_high(12)
            .mesh_outbound_min(1)
            .message_id_fn(|msg| {
                let mut h = DefaultHasher::new();
                msg.data.hash(&mut h);
                gossipsub::MessageId::from(h.finish().to_be_bytes().to_vec())
            })
            .build()
            .expect("valid gossipsub config");

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .expect("valid gossipsub behaviour");

        let mut kad_config = kad::Config::default();
        kad_config.set_query_timeout(Duration::from_secs(30));

        let mut kademlia =
            kad::Behaviour::with_config(local_peer_id, MemoryStore::new(local_peer_id), kad_config);
        kademlia.set_mode(Some(kad::Mode::Server));

        Self {
            relay_client,
            relay_server: relay::Behaviour::new(local_peer_id, relay::Config::default()),
            dcutr: dcutr::Behaviour::new(local_peer_id),
            autonat: autonat::Behaviour::new(local_peer_id, autonat::Config::default()),
            identify: identify::Behaviour::new(identify::Config::new(
                "/nocap/1.0.0".into(),
                keypair.public(),
            )),
            gossipsub,
            kademlia,
            mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
                .expect("mDNS setup failed"),
        }
    }
}
