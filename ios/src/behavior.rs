use libghost::traits::{MeshBehaviour, MeshEvent};
use libp2p::{identity::PublicKey, kad, relay, swarm::NetworkBehaviour};

#[derive(NetworkBehaviour)]
pub struct ClientBehavior {
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub relay_client: relay::client::Behaviour,
}

impl ClientBehavior {
    pub fn new(public_key: PublicKey, relay_client: relay::client::Behaviour) -> Self {
        let peer_id = public_key.to_peer_id();
        let store = kad::store::MemoryStore::new(peer_id);
        let kademlia = kad::Behaviour::with_config(peer_id, store, kad::Config::default());
        Self {
            kademlia,
            relay_client,
        }
    }
}

impl MeshBehaviour for ClientBehavior {
    fn on_relay_accepted(&mut self) {
        let _ = self.kademlia.bootstrap();
    }

    fn translate_event(event: &ClientBehaviorEvent) -> Option<MeshEvent> {
        match event {
            ClientBehaviorEvent::Kademlia(kad::Event::RoutingUpdated {
                peer, addresses, ..
            }) => addresses
                .iter()
                .next()
                .map(|addr| MeshEvent::PeerDiscovered {
                    peer_id: peer.to_string(),
                    addr: addr.to_string(),
                }),

            ClientBehaviorEvent::RelayClient(relay::client::Event::ReservationReqAccepted {
                ..
            }) => Some(MeshEvent::RelayReservationAccepted),

            // Relay client has no failure variant — failures are caught
            // at the swarm level in node.rs as OutgoingConnectionError
            _ => None,
        }
    }
}
