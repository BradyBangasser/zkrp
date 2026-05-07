use std::time::Duration;

use futures::StreamExt;
use libp2p::{Multiaddr, SwarmBuilder, gossipsub, noise, swarm::SwarmEvent, tcp, yamux};
use prost::Message;
use tokio::sync::mpsc;
use tracing::warn;

use libghost::{
    identity::NodeIdentity,
    traits::{MeshBehaviour, MeshEvent},
    transport::TransportConfig,
};

use libghost::protocols::ghost::v0::{self, GhostEnvelope};

enum SwarmCommand {
    Publish {
        topic: gossipsub::IdentTopic,
        envelope: GhostEnvelope,
    },
    Subscribe(gossipsub::IdentTopic),
}

enum SwarmNotification {
    MeshEvent(MeshEvent),
    Gossip(GhostEnvelope),
}

pub struct MeshNode {
    cmd_tx: mpsc::Sender<SwarmCommand>,
    notify_rx: mpsc::Receiver<SwarmNotification>,
    events: Vec<MeshEvent>,
    messages: Vec<GhostEnvelope>,
}

impl MeshNode {
    pub async fn start<B, F>(
        identity: NodeIdentity,
        relay_addr: String,
        config: TransportConfig,
        make_behaviour: F,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        B: MeshBehaviour + Send + 'static,
        B::ToSwarm: Send,
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

        let mut bootstrap_events: Vec<MeshEvent> = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

        loop {
            tokio::select! {
                event = swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(ref b_event) => {
                        if let Some(mesh_event) = B::translate_event(b_event) {
                            match &mesh_event {
                                MeshEvent::RelayReservationAccepted => {
                                    swarm.behaviour_mut().on_relay_accepted();
                                    bootstrap_events.push(mesh_event);
                                    break;
                                }
                                _ => bootstrap_events.push(mesh_event),
                            }
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { .. } => {
                        bootstrap_events.push(MeshEvent::RelayReservationFailed);
                        break;
                    }
                    _ => {}
                },
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<SwarmCommand>(32);
        let (notify_tx, notify_rx) = mpsc::channel::<SwarmNotification>(64);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = swarm.select_next_some() => {
                        if let SwarmEvent::Behaviour(b_event) = event {
                            // Extract all data before any await point so b_event
                            // does not need to be Send or Sync.
                            let mesh_event = B::translate_event(&b_event);
                            let gossip_bytes = B::extract_gossip(&b_event);
                            drop(b_event);

                            if let Some(mesh_event) = mesh_event {
                                let _ = notify_tx
                                    .send(SwarmNotification::MeshEvent(mesh_event))
                                    .await;
                            }

                            if let Some(bytes) = gossip_bytes {
                                match GhostEnvelope::decode(bytes.as_slice()) {
                                    Ok(envelope) => {
                                        let _ = notify_tx
                                            .send(SwarmNotification::Gossip(envelope))
                                            .await;
                                    }
                                    Err(e) => {
                                        tracing::debug!("failed to decode envelope: {e}");
                                    }
                                }
                            }
                        }
                    },

                    Some(cmd) = cmd_rx.recv() => match cmd {
                        SwarmCommand::Publish { topic, envelope } => {
                            let bytes = envelope.encode_to_vec();
                            if let Err(e) = swarm.behaviour_mut().publish(topic, bytes) {
                                warn!("gossipsub publish failed: {e}");
                            }
                        }
                        SwarmCommand::Subscribe(topic) => {
                            if let Err(e) = swarm.behaviour_mut().subscribe_topic(topic) {
                                warn!("gossipsub subscribe failed: {e}");
                            }
                        }
                    },
                }
            }
        });

        Ok(Self {
            cmd_tx,
            notify_rx,
            events: bootstrap_events,
            messages: Vec::new(),
        })
    }

    // ── Public API ────────────────────────────────────────────────────────────

    pub async fn subscribe(&self, topic_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let topic = gossipsub::IdentTopic::new(topic_name);
        self.cmd_tx.send(SwarmCommand::Subscribe(topic)).await?;
        Ok(())
    }

    pub async fn send_message(
        &self,
        topic_name: &str,
        payload: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let topic = gossipsub::IdentTopic::new(topic_name);
        let envelope = v0::encode(&payload);
        self.cmd_tx
            .send(SwarmCommand::Publish { topic, envelope })
            .await?;
        Ok(())
    }

    fn flush(&mut self) {
        while let Ok(n) = self.notify_rx.try_recv() {
            match n {
                SwarmNotification::MeshEvent(e) => self.events.push(e),
                SwarmNotification::Gossip(m) => self.messages.push(m),
            }
        }
    }

    pub fn drain_events(&mut self) -> Vec<MeshEvent> {
        self.flush();
        std::mem::take(&mut self.events)
    }

    pub fn drain_messages(&mut self) -> Vec<GhostEnvelope> {
        self.flush();
        std::mem::take(&mut self.messages)
    }
}
