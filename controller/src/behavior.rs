use libp2p::{PeerId, autonat, identify, identity::Keypair, relay, swarm::NetworkBehaviour};
use std::time::Duration;

#[derive(NetworkBehaviour)]
pub struct RelayBehavior {
    pub relay_server: relay::Behaviour,
    pub identify: identify::Behaviour,
    pub autonat: autonat::Behaviour,
}

impl RelayBehavior {
    pub fn new(keypair: &Keypair) -> Self {
        let local_peer_id = PeerId::from(keypair.public());

        Self {
            relay_server: relay::Behaviour::new(
                local_peer_id,
                relay::Config {
                    max_circuit_duration: Duration::from_secs(60 * 60),
                    max_circuit_bytes: 0,
                    max_circuits: 256,
                    max_circuits_per_peer: 16,
                    ..Default::default()
                },
            ),
            identify: identify::Behaviour::new(identify::Config::new(
                "/nocap/1.0.0".into(),
                keypair.public(),
            )),
            autonat: autonat::Behaviour::new(local_peer_id, autonat::Config::default()),
        }
    }
}
