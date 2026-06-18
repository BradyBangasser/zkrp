use libp2p::{Multiaddr, PeerId, gossipsub, swarm::NetworkBehaviour};

#[derive(Debug, Clone)]
pub enum MeshEvent {
    PeerDiscovered { peer_id: PeerId, addr: Multiaddr },
    PeerLost { peer_id: PeerId, addr: Multiaddr },
    RelayReservationAccepted { peer_id: PeerId },
    RelayReservationFailed,
    Message { conversation: String, msg: String },
}

pub trait MeshBehaviour: NetworkBehaviour {
    fn on_relay_accepted(&mut self, _relay_peer_id: PeerId, _relay_addr: Multiaddr) {}
    fn translate_event(event: &Self::ToSwarm) -> Option<MeshEvent>;
    fn extract_gossip(_event: &Self::ToSwarm) -> Option<(String, Vec<u8>, PeerId)> {
        None
    }
    fn publish(
        &mut self,
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    ) -> Result<(), gossipsub::PublishError>;

    fn subscribe_topic(
        &mut self,
        topic: gossipsub::IdentTopic,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
