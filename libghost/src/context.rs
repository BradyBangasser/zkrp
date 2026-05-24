use crate::{
    codec::Codec,
    handler::{ConnectionStatus, DisconnectReason, EventHandler, SendFailReason, ZRPEvent},
    keybundle::KeyBundle,
    protocols::ghost::v0::{GhostEnvelope, GhostMessage, encode},
};
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use zeroize::Zeroize;

const MAX_ATTEMPTS: u32 = 8;

fn backoff_delay(attempt: u32) -> Option<Duration> {
    if attempt >= MAX_ATTEMPTS {
        return None;
    }
    let secs = (2u64.pow(attempt - 1)).min(60);
    Some(Duration::from_secs(secs))
}

struct PendingMessage {
    topic_hash: String,
    payload: Vec<u8>,
    attempts: u32,
    next_retry: tokio::time::Instant,
}

async fn swarm_task<B>(
    mut swarm: libp2p::Swarm<B>,
    relay_addrs: Vec<String>,
    transport_config: crate::transport::TransportConfig,
    mut cmd_rx: mpsc::Receiver<SwarmCommand>,
    raw_tx: mpsc::Sender<RawEvent>,
) where
    B: crate::traits::MeshBehaviour + Send + 'static,
    B::ToSwarm: Send,
{
    use futures::StreamExt;
    use libp2p::swarm::SwarmEvent;

    let mut retry_interval = tokio::time::interval(Duration::from_millis(500));
    let mut outbox: Vec<PendingMessage> = Vec::new();

    swarm
        .listen_on(transport_config.tcp_listen_addr().parse().unwrap())
        .unwrap();
    swarm
        .listen_on(transport_config.quic_listen_addr().parse().unwrap())
        .unwrap();

    for relay in &relay_addrs {
        let addr: libp2p::Multiaddr = relay.parse().unwrap();
        let _ = swarm.dial(addr);
    }

    loop {
        tokio::select! {
            biased;
            _ = retry_interval.tick() => {
                let now = tokio::time::Instant::now();
                let mut still_pending = Vec::new();

                for mut msg in outbox.drain(..) {
                    if now < msg.next_retry {
                        still_pending.push(msg);
                        continue;
                    }

                    let topic = libp2p::gossipsub::IdentTopic::new(&msg.topic_hash);
                    match swarm.behaviour_mut().publish(topic, msg.payload.clone()) {
                        Ok(_) => {
                            tracing::info!("publish succeeded after {} attempts", msg.attempts + 1);
                        }

                        Err(libp2p::gossipsub::PublishError::NoPeersSubscribedToTopic) => {
                            msg.attempts += 1;
                            if let Some(delay) = backoff_delay(msg.attempts) {
                                msg.next_retry = now + delay;
                                tracing::debug!(
                                    "publish retry {} in {:?}", msg.attempts, delay
                                );
                                still_pending.push(msg);
                            } else {
                                tracing::warn!("publish giving up after {} attempts", msg.attempts);
                                let _ = raw_tx.send(RawEvent::PublishFailed {
                                    topic_hash: msg.topic_hash.clone(),
                                }).await;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("publish failed (non-retryable): {e}");
                            let _ = raw_tx.send(RawEvent::PublishFailed {
                                topic_hash: msg.topic_hash.clone(),
                            }).await;
                        }
                    }
                }

                outbox = still_pending;
            }
        event = swarm.select_next_some() => {
            match event {
                SwarmEvent::Behaviour(b_event) => {
                    if let Some(mesh_event) = B::translate_event(&b_event) {
                        match mesh_event {
                            crate::traits::MeshEvent::PeerDiscovered { peer_id, addr } => {
                                let _ = raw_tx.send(RawEvent::PeerDiscovered {
                                    peer_id, addr
                                }).await;
                            }
                            crate::traits::MeshEvent::RelayReservationAccepted { peer_id } => {
                                let _ = raw_tx.send(RawEvent::RelayAccepted { relay_addr: peer_id }).await;
                            }
                            crate::traits::MeshEvent::RelayReservationFailed => {
                                let _ = raw_tx.send(RawEvent::RelayLost).await;
                            }
                            _ => {}
                        }
                    }

                    if let Some((bytes, propagation_src)) = B::extract_gossip(&b_event) {
                        let topic = String::new();
                        let _ = raw_tx.send(RawEvent::GossipMessage { topic, bytes, propagation_src }).await;
                    }
                }
                SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                    tracing::info!("Connection closed: {} cause: {:?}", peer_id, cause);
                    let _ = raw_tx.send(RawEvent::PeerLost {
                        peer_id
                    }).await;
                }
                SwarmEvent::ConnectionEstablished { .. } => {
                    // dial circuit if this is a relay
                }
                _ => {}
            }
        }

        Some(cmd) = cmd_rx.recv() => {
            match cmd {
                SwarmCommand::Publish { topic_hash, payload } => {
                    outbox.push(PendingMessage {
                        topic_hash,
                        payload,
                        attempts: 0,
                        next_retry: tokio::time::Instant::now(),
                    });
                }
                    SwarmCommand::Subscribe { topic_hash } => {
                        let topic = libp2p::gossipsub::IdentTopic::new(&topic_hash);
                        if let Err(e) = swarm.behaviour_mut().subscribe_topic(topic) {
                            tracing::warn!("subscribe failed: {e}");
                        }
                    }
                    SwarmCommand::Shutdown => break,
                    _ => {}
                }
            }
        }
    }
}

async fn load_key_bundle() -> KeyBundle {
    KeyBundle {}
}

async fn crypto_task(
    mut raw_rx: mpsc::Receiver<RawEvent>,
    mut crypto_rx: mpsc::Receiver<SwarmCommand>,
    swarm_tx: mpsc::Sender<SwarmCommand>,
    handlers: Arc<Mutex<HashMap<String, Arc<dyn EventHandler>>>>,
    codecs: Arc<Mutex<HashMap<u16, Codec>>>,
) {
    let kb = load_key_bundle().await;
    loop {
        tokio::select! {
            Some(event) = raw_rx.recv() => {
                let e: ZRPEvent = match event {
                    RawEvent::GossipMessage { bytes, propagation_src, topic } => {
                        let envelope = GhostEnvelope::decode(bytes.as_ref()).unwrap();
                        // TODO: RATCHET AND decryption
                        let msg: GhostMessage = match postcard::from_bytes(&envelope.payload) {
                            Ok(m) => m,
                            Err(e) => {
                                tracing::warn!("failed to deserialize message: {e}");
                                continue;
                            }
                        };
                        if codecs.lock().await.is_empty() {
                            tracing::error!("No registered codecs");
                        }
                        ZRPEvent::Message {
                            conversation: topic,
                            peer_id: propagation_src,
                            content_type: 0,
                            payload: msg.payload.clone(),
                        }
                    }
                    RawEvent::PeerDiscovered { peer_id, addr } => {
                        ZRPEvent::PeerConnected { peer_id, addr }
                    }
                    RawEvent::PeerLost { peer_id } => {
                        ZRPEvent::PeerDisconnected {
                            peer_id,
                            reason: DisconnectReason::Clean,
                        }
                    }
                    RawEvent::RelayAccepted { relay_addr } => {
                        ZRPEvent::ConnectionStatus(ConnectionStatus::Connected {
                        relay: relay_addr,
                    })
                }
                RawEvent::RelayLost => {
                    ZRPEvent::ConnectionStatus(ConnectionStatus::Disconnected)
                }
                RawEvent::PublishFailed { topic_hash } => {
                    ZRPEvent::MessageSendFailed {
                        conversation: topic_hash,
                        reason: SendFailReason::NoPeersSubscribed,
                    }
                }
            };
            let event = Arc::new(e);
            for handler in handlers.lock().await.values() {
                let h = handler.clone();
                let e = event.clone();
                tokio::spawn(async move { h.handle(&e) });
            }
        }

                Some(cmd) = crypto_rx.recv() => {
                    match cmd {
                        SwarmCommand::Publish { topic_hash, payload } => {
                            if let Ok(message) = GhostMessage::new(&kb, &payload) {
                                let payload = postcard::to_allocvec(&message).unwrap();
                                let envelope = encode(&payload);
                                let mut payload = Vec::new();
                                envelope.encode(&mut payload).unwrap();

                                let _ = swarm_tx.send(SwarmCommand::Publish {
                                    topic_hash,
                                    payload,
                                }).await;
                            } else {
                            todo!()
                        }
                            // 1. Serialize with postcard
                            // 2. Encrypt with ratchet session
                            // 3. Wrap in GhostEnvelope
                            // 4. Forward to swarm task
                        }
                        SwarmCommand::Subscribe { topic_hash } => {
                            // Pass through — no crypto needed for subscribe
                            let _ = swarm_tx.send(SwarmCommand::Subscribe { topic_hash }).await;
                        }
                        SwarmCommand::Shutdown => {
                            let _ = swarm_tx.send(SwarmCommand::Shutdown).await;
                            break;
                        }
                        _ => {}
                    }
                }
        }
    }
}

pub(crate) enum SwarmCommand {
    Publish {
        topic_hash: String,
        payload: Vec<u8>,
    },
    Subscribe {
        topic_hash: String,
    },
    #[allow(unused)]
    Unsubscribe {
        topic_hash: String,
    },
    Shutdown,
}

pub(crate) enum RawEvent {
    GossipMessage {
        propagation_src: libp2p::PeerId,
        topic: String,
        bytes: Vec<u8>,
    },
    PeerDiscovered {
        peer_id: libp2p::PeerId,
        addr: libp2p::Multiaddr,
    },
    PeerLost {
        peer_id: libp2p::PeerId,
    },
    RelayAccepted {
        relay_addr: libp2p::PeerId,
    },
    RelayLost,
    PublishFailed {
        topic_hash: String,
    },
}

#[derive(Clone)]
pub struct ZRPHandle {
    cmd_tx: mpsc::Sender<SwarmCommand>,
}

impl ZRPHandle {
    pub async fn publish(&self, topic_hash: String, payload: Vec<u8>) {
        let _ = self
            .cmd_tx
            .send(SwarmCommand::Publish {
                topic_hash,
                payload,
            })
            .await;
    }

    pub async fn subscribe(&self, topic_hash: String) {
        let _ = self
            .cmd_tx
            .send(SwarmCommand::Subscribe { topic_hash })
            .await;
    }

    pub async fn unsubscribe(&self, topic_hash: String) {
        let _ = self
            .cmd_tx
            .send(SwarmCommand::Unsubscribe { topic_hash })
            .await;
    }

    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(SwarmCommand::Shutdown).await;
    }
}

pub struct ZRPContext {
    codecs: Arc<Mutex<HashMap<u16, Codec>>>,
    handlers: Arc<Mutex<HashMap<String, Arc<dyn EventHandler>>>>,
}

impl ZRPContext {
    pub async fn register_codec(&mut self, id: u16, codec: Codec) {
        self.codecs.lock().await.insert(id, codec);
    }

    pub async fn register_handler(
        &mut self,
        name: impl Into<String>,
        handler: impl EventHandler + 'static,
    ) {
        self.handlers
            .lock()
            .await
            .insert(name.into(), Arc::new(handler));
    }

    pub fn remove_handler(&mut self, name: &str) {
        let mut handlers = self.handlers.blocking_lock();
        handlers.remove(name);
    }

    pub async fn start<B, F>(
        self,
        identity: crate::identity::NodeIdentity,
        relay_addrs: Vec<String>,
        transport_config: crate::transport::TransportConfig,
        make_behavior: F,
    ) -> Result<ZRPHandle, Box<dyn std::error::Error>>
    where
        B: crate::traits::MeshBehaviour + Send + 'static,
        B::ToSwarm: Send,
        F: FnOnce(&libp2p::identity::Keypair, libp2p::relay::client::Behaviour) -> B,
    {
        let (crypto_tx, crypto_rx) = mpsc::channel::<SwarmCommand>(32);
        let (swarm_tx, swarm_rx) = mpsc::channel::<SwarmCommand>(32);
        let (raw_tx, raw_rx) = mpsc::channel::<RawEvent>(64);

        let swarm = libp2p::SwarmBuilder::with_existing_identity(identity.keypair)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_quic()
            .with_dns()?
            .with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)?
            .with_behaviour(|key, relay_client| make_behavior(key, relay_client))?
            .with_swarm_config(|cfg| {
                cfg.with_idle_connection_timeout(transport_config.idle_connection_timeout)
            })
            .build();

        tokio::spawn(swarm_task::<B>(
            swarm,
            relay_addrs,
            transport_config,
            swarm_rx,
            raw_tx,
        ));

        let handlers = Arc::clone(&self.handlers);
        let codecs = Arc::clone(&self.codecs);
        tokio::spawn(crypto_task(raw_rx, crypto_rx, swarm_tx, handlers, codecs));

        Ok(ZRPHandle { cmd_tx: crypto_tx })
    }
}

impl Default for ZRPContext {
    fn default() -> Self {
        let mut codecs = HashMap::new();

        codecs.insert(0x0001, Codec::text());

        Self {
            codecs: Arc::new(Mutex::new(codecs)),
            handlers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Zeroize for ZRPContext {
    fn zeroize(&mut self) {}
}

impl Drop for ZRPContext {
    fn drop(&mut self) {
        self.zeroize();
    }
}
