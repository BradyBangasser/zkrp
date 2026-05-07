use libp2p::swarm::NetworkBehaviour;

#[derive(Debug, Clone)]
pub enum MeshEvent {
    PeerDiscovered { peer_id: String, addr: String },
    PeerLost { peer_id: String, addr: String },
    RelayReservationAccepted,
    RelayReservationFailed,
}

pub trait MeshBehaviour: NetworkBehaviour {
    fn on_relay_accepted(&mut self) {}

    fn translate_event(event: &Self::ToSwarm) -> Option<MeshEvent>;
}
