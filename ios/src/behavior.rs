use libghost::traits::{MeshBehaviour, MeshEvent};
use libp2p::{gossipsub, identity::PublicKey, kad, relay, swarm::NetworkBehaviour};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

#[derive(NetworkBehaviour)]
pub struct ClientBehavior {
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub relay_client: relay::client::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
}

impl ClientBehavior {
    pub fn new(
        public_key: PublicKey,
        relay_client: relay::client::Behaviour,
        keypair: &libp2p::identity::Keypair,
    ) -> Self {
        let peer_id = public_key.to_peer_id();
        let store = kad::store::MemoryStore::new(peer_id);
        let kademlia = kad::Behaviour::with_config(peer_id, store, kad::Config::default());

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10))
            .validation_mode(gossipsub::ValidationMode::Strict)
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

        Self {
            kademlia,
            relay_client,
            gossipsub,
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

            _ => None,
        }
    }

    fn extract_gossip(event: &ClientBehaviorEvent) -> Option<Vec<u8>> {
        match event {
            ClientBehaviorEvent::Gossipsub(gossipsub::Event::Message { message, .. }) => {
                Some(message.data.clone())
            }
            _ => None,
        }
    }
    fn publish(
        &mut self,
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
}
