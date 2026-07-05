use libp2p::{
    PeerId, autonat, identify,
    identity::Keypair,
    kad::{self, store::MemoryStore},
    relay,
    swarm::NetworkBehaviour,
};
use std::time::Duration;

#[derive(NetworkBehaviour)]
pub struct RelayBehavior {
    pub relay_server: relay::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub autonat: autonat::Behaviour,
}

impl RelayBehavior {
    pub fn new(keypair: &Keypair) -> Self {
        let local_peer_id = PeerId::from(keypair.public());

        let mut kad_config = kad::Config::default();
        kad_config.set_query_timeout(Duration::from_secs(30));
        let mut kademlia =
            kad::Behaviour::with_config(local_peer_id, MemoryStore::new(local_peer_id), kad_config);
        kademlia.set_mode(Some(kad::Mode::Server));

        let relay_server = relay::Behaviour::new(
            local_peer_id,
            relay::Config {
                max_circuit_duration: Duration::from_secs(60 * 60),
                max_circuit_bytes: 0,
                max_circuits: 256,
                max_circuits_per_peer: 16,
                ..Default::default()
            },
        );

        let identify = identify::Behaviour::new(
            identify::Config::new("/nocap/1.0.0".into(), keypair.public())
                .with_push_listen_addr_updates(true),
        );

        let autonat = autonat::Behaviour::new(local_peer_id, autonat::Config::default());

        Self {
            relay_server,
            identify,
            kademlia,
            autonat,
        }
    }
}
