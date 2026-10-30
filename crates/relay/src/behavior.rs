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
    pub admit: libghost::admit::AdmitBehaviour,
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
                // libp2p's defaults also cap each IP at 60 a minute, but a
                // whole campus can share one NAT address. Keep the per-peer
                // limits; admission is the real gate, and the per-IP ceiling
                // only stops one host from flooding the relay.
                reservation_rate_limiters: Vec::new(),
                circuit_src_rate_limiters: Vec::new(),
                ..Default::default()
            }
            .reservation_rate_per_peer(per(30), Duration::from_secs(120))
            .reservation_rate_per_ip(per(1200), Duration::from_secs(60))
            .circuit_src_per_peer(per(30), Duration::from_secs(120))
            .circuit_src_per_ip(per(1200), Duration::from_secs(60)),
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
            admit: libghost::admit::relay_behaviour(),
        }
    }
}

fn per(n: u32) -> std::num::NonZeroU32 {
    std::num::NonZeroU32::new(n).expect("rate limits are positive")
}
