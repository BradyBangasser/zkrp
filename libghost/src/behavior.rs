use crate::traits::{MeshBehaviour, MeshEvent};
use libp2p::{Multiaddr, PeerId, identify};
use libp2p::{gossipsub, identity::PublicKey, kad, relay, swarm::NetworkBehaviour};
use std::time::Duration;
use tracing::info;

#[derive(NetworkBehaviour)]
pub struct ClientBehavior {
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub relay_client: relay::client::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub identify: identify::Behaviour,
}

impl ClientBehavior {
    pub fn new(
        public_key: PublicKey,
        relay_client: relay::client::Behaviour,
        keypair: &libp2p::identity::Keypair,
    ) -> Self {
        let peer_id = public_key.to_peer_id();
        let store = kad::store::MemoryStore::new(peer_id);
        let mut kademlia = kad::Behaviour::with_config(peer_id, store, kad::Config::default());
        kademlia.set_mode(Some(kad::Mode::Client));

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .mesh_n(2)
            .mesh_n_low(1)
            .mesh_n_high(4)
            .mesh_outbound_min(1)
            .message_id_fn(|msg| {
                use sha3::{Digest, Sha3_512};
                let mut hasher = Sha3_512::default();
                Digest::update(&mut hasher, &msg.data);
                gossipsub::MessageId::from(hasher.finalize().to_vec())
            })
            .build()
            .expect("valid gossipsub config");

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .expect("valid gossipsub behaviour");

        let identify = identify::Behaviour::new(identify::Config::new(
            "/nocap/1.0.0".to_string(),
            public_key.clone(),
        ));

        Self {
            kademlia,
            relay_client,
            gossipsub,
            identify,
        }
    }
}

impl MeshBehaviour for ClientBehavior {
    fn on_relay_accepted(&mut self, relay_peer_id: PeerId, relay_addr: Multiaddr) {
        info!("ADDING REPLAY PEER {}", relay_peer_id);
        self.kademlia.add_address(&relay_peer_id, relay_addr);
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
                    peer_id: *peer,
                    addr: addr.clone(),
                }),

            ClientBehaviorEvent::RelayClient(relay::client::Event::ReservationReqAccepted {
                relay_peer_id,
                ..
            }) => Some(MeshEvent::RelayReservationAccepted {
                peer_id: *relay_peer_id,
            }),

            ClientBehaviorEvent::Identify(identify::Event::Received { peer_id, info, .. }) => {
                Some(MeshEvent::Identify {
                    peer_id: *peer_id,
                    listen_addrs: info.listen_addrs.clone(),
                })
            }

            _ => None,
        }
    }

    fn extract_gossip(event: &ClientBehaviorEvent) -> Option<(String, Vec<u8>, PeerId)> {
        match event {
            ClientBehaviorEvent::Gossipsub(gossipsub::Event::Message {
                message,
                propagation_source,
                ..
            }) => Some((
                message.topic.clone().into_string(),
                message.data.clone(),
                *propagation_source,
            )),
            _ => None,
        }
    }
    fn publish(
        &mut self,
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    ) -> Result<(), gossipsub::PublishError> {
        self.gossipsub.publish(topic, data)?;
        Ok(())
    }

    fn subscribe_topic(
        &mut self,
        topic: gossipsub::IdentTopic,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.gossipsub.subscribe(&topic)?;
        Ok(())
    }

    fn on_identify_received(&mut self, peer_id: PeerId, listen_addrs: Vec<Multiaddr>) {
        for addr in listen_addrs {
            self.kademlia.add_address(&peer_id, addr);
        }
    }
}
