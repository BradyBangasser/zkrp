use std::time::Duration;

use futures::StreamExt;
use libp2p::{Multiaddr, SwarmBuilder, noise, swarm::SwarmEvent, tcp, yamux};

use libghost::{
    identity::NodeIdentity,
    traits::{MeshBehaviour, MeshEvent},
    transport::TransportConfig,
};

pub struct MeshNode {
    events: Vec<MeshEvent>,
}

impl MeshNode {
    pub async fn start<B, F>(
        identity: NodeIdentity,
        relay_addr: String,
        config: TransportConfig,
        make_behaviour: F,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        B: MeshBehaviour,
        F: FnOnce(&libp2p::identity::Keypair, libp2p::relay::client::Behaviour) -> B,
    {
        let relay_multiaddr: Multiaddr = relay_addr.parse()?;

        let mut swarm = SwarmBuilder::with_existing_identity(identity.keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_quic()
            .with_dns()?
            .with_relay_client(noise::Config::new, yamux::Config::default)?
            .with_behaviour(|key, relay_client| make_behaviour(key, relay_client))?
            .with_swarm_config(|cfg| {
                cfg.with_idle_connection_timeout(config.idle_connection_timeout)
            })
            .build();

        swarm.listen_on(config.tcp_listen_addr().parse()?)?;
        swarm.listen_on(config.quic_listen_addr().parse()?)?;
        swarm.dial(relay_multiaddr)?;

        let mut collected = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

        loop {
            tokio::select! {
                event = swarm.select_next_some() => {
                    match event {
                        SwarmEvent::Behaviour(ref b_event) => {
                            if let Some(mesh_event) = B::translate_event(b_event) {
                                match &mesh_event {
                                    MeshEvent::RelayReservationAccepted => {
                                        swarm.behaviour_mut().on_relay_accepted();
                                        collected.push(mesh_event);
                                        break;
                                    }
                                    _ => collected.push(mesh_event),
                                }
                            }
                        }
                        SwarmEvent::OutgoingConnectionError { .. } => {
                            collected.push(MeshEvent::RelayReservationFailed);
                            break;
                        }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }

        Ok(Self { events: collected })
    }

    pub fn drain_events(&mut self) -> Vec<MeshEvent> {
        std::mem::take(&mut self.events)
    }
}
