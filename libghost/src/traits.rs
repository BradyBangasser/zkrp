use libp2p::{Multiaddr, PeerId, gossipsub, swarm::NetworkBehaviour};

#[derive(Debug, Clone)]
pub enum MeshEvent {
    PeerDiscovered { peer_id: String, addr: String },
    PeerLost { peer_id: String, addr: String },
    RelayReservationAccepted { relay_addr: String },
    RelayReservationFailed,
}

pub trait MeshBehaviour: NetworkBehaviour {
    fn on_relay_accepted(&mut self, relay_peer_id: PeerId, relay_addr: Multiaddr) {}
    fn translate_event(event: &Self::ToSwarm) -> Option<MeshEvent>;
    fn extract_gossip(_event: &Self::ToSwarm) -> Option<Vec<u8>> {
        None
    }
    fn publish(
        &mut self,
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn subscribe_topic(
        &mut self,
        topic: gossipsub::IdentTopic,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
