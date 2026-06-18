use crate::blob::{BlobManager, BlobManifest, ChunkRequest, ChunkResponse, ChunkStoreRequest};
use crate::relay::RelayClient;
use crate::{
    codec::{self, Codec},
    handler::{ConnectionStatus, DisconnectReason, EventHandler, SendFailReason, ZRPEvent},
    keybundle::KeyBundle,
    protocols::ghost::v0::{GhostEnvelope, GhostMessage, encode},
    store::GhostStore,
};
use libp2p::dns::{ResolverConfig, ResolverOpts};
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use ulid::Ulid;
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
    id: String,
    topic: String,
    payload: Vec<u8>,
    attempts: u32,
    next_retry: tokio::time::Instant,
    content_type: u16,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OutboxEntry {
    id: String,
    topic: String,
    payload: Vec<u8>,
    attempts: u32,
    content_type: u16,
}

impl From<&PendingMessage> for OutboxEntry {
    fn from(m: &PendingMessage) -> Self {
        Self {
            id: m.id.clone(),
            topic: m.topic.clone(),
            payload: m.payload.clone(),
            attempts: m.attempts,
            content_type: m.content_type,
        }
    }
}

async fn swarm_task<B>(
    mut swarm: libp2p::Swarm<B>,
    relay_addrs: Vec<String>,
    transport_config: crate::transport::TransportConfig,
    mut cmd_rx: mpsc::Receiver<SwarmCommand>,
    raw_tx: mpsc::Sender<RawEvent>,
    store: Arc<dyn GhostStore>,
) where
    B: crate::traits::MeshBehaviour + Send + 'static,
    B::ToSwarm: Send,
{
    use futures::StreamExt;
    use libp2p::swarm::SwarmEvent;

    let mut circuit_reserved: std::collections::HashSet<libp2p::PeerId> =
        std::collections::HashSet::new();

    let mut retry_interval = tokio::time::interval(Duration::from_millis(500));
    let mut outbox: Vec<PendingMessage> = store
        .list("ghost/outbox/")
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<OutboxEntry>(&bytes).ok())
        .map(|e| PendingMessage {
            id: e.id,
            topic: e.topic,
            payload: e.payload,
            attempts: e.attempts,
            next_retry: tokio::time::Instant::now(),
            content_type: e.content_type,
        })
        .collect();

    if !outbox.is_empty() {
        tracing::info!("Loaded {} pending messages from outbox", outbox.len());
    }

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

                    let topic = libp2p::gossipsub::IdentTopic::new(&msg.topic);
                    match swarm.behaviour_mut().publish(topic, msg.payload.clone()) {
                        Ok(_) => {
                            store.delete(&format!("ghost/outbox/{}", msg.id));
                            tracing::info!("publish succeeded after {} attempts", msg.attempts + 1);
                        }

                        Err(libp2p::gossipsub::PublishError::NoPeersSubscribedToTopic) => {
                            msg.attempts += 1;
                            if let Some(delay) = backoff_delay(msg.attempts) {
                                msg.next_retry = now + delay;
                                tracing::debug!(
                                    "publish retry {} in {:?}", msg.attempts, delay
                                );
                                store.set(
                                    &format!("ghost/outbox/{}", msg.id),
                                    postcard::to_allocvec(&OutboxEntry::from(&msg)).unwrap(),
                                );
                                still_pending.push(msg);
                            } else {
                                store.delete(&format!("ghost/outbox/{}", msg.id));
                                tracing::warn!("publish giving up after {} attempts", msg.attempts);
                                let _ = raw_tx.send(RawEvent::PublishFailed {
                                    topic: msg.topic.clone(),
                                }).await;
                            }
                        }
                        Err(e) => {
                            store.delete(&format!("ghost/outbox/{}", msg.id));
                            tracing::warn!("publish failed (non-retryable): {e}");
                            let _ = raw_tx.send(RawEvent::PublishFailed {
                                topic: msg.topic.clone(),
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

                    if let Some((topic, bytes, propagation_src)) = B::extract_gossip(&b_event) {
                        let _ = raw_tx.send(RawEvent::GossipMessage { topic, bytes, propagation_src }).await;
                    }
                }
                SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                    circuit_reserved.remove(&peer_id);
                    tracing::info!("Connection closed: {} cause: {:?}", peer_id, cause);
                    let _ = raw_tx.send(RawEvent::PeerLost {
                        peer_id
                    }).await;
                }

                SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                    let addr = match &endpoint {
                        libp2p::core::ConnectedPoint::Dialer { address, .. } => address.clone(),
                        libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
                    };

                    let _ = raw_tx.send(RawEvent::PeerDiscovered { peer_id, addr }).await;

                    if !circuit_reserved.contains(&peer_id) {
                        let circuit_addr = relay_addrs.iter().find_map(|r| {
                            let ma = r.parse::<libp2p::Multiaddr>().ok()?;
                            let has_peer = ma.iter().any(|p| {
                                matches!(p, libp2p::multiaddr::Protocol::P2p(id) if id == peer_id)
                            });
                            if has_peer { Some(ma) } else { None }
                        });

                        if let Some(ma) = circuit_addr {
                            circuit_reserved.insert(peer_id);
                            let circuit = format!("{}/p2p-circuit", ma);
                            match circuit.parse::<libp2p::Multiaddr>() {
                                Ok(addr) => {
                                    tracing::info!("Requesting circuit reservation via {}", addr);
                                    if let Err(e) = swarm.dial(addr) {
                                        tracing::warn!("Circuit dial failed: {}", e);
                                    }
                                }
                                Err(e) => tracing::warn!("Invalid circuit addr: {}", e),
                            }
                        }
                    }
                }

                _ => {}
            }
        }

        Some(cmd) = cmd_rx.recv() => {
            match cmd {
                    SwarmCommand::Publish { topic, payload, content_type} => {
                        let msg = PendingMessage {
                            id: Ulid::new().to_string(),
                            topic,
                            payload,
                            attempts: 0,
                            next_retry: tokio::time::Instant::now(),
                            content_type
                        };
                        store.set(
                            &format!("ghost/outbox/{}", msg.id),
                            postcard::to_allocvec(&OutboxEntry::from(&msg)).unwrap(),
                        );
                        outbox.push(msg);
                    }
                    SwarmCommand::Subscribe { topic } => {
                        let topic = libp2p::gossipsub::IdentTopic::new(&topic);
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

async fn load_key_bundle(store: &Arc<dyn GhostStore>) -> KeyBundle {
    KeyBundle::load_or_generate(store)
}

#[allow(unused)]
async fn crypto_task(
    mut raw_rx: mpsc::Receiver<RawEvent>,
    mut crypto_rx: mpsc::Receiver<SwarmCommand>,
    swarm_tx: mpsc::Sender<SwarmCommand>,
    crypto_tx: mpsc::Sender<SwarmCommand>,
    handlers: Arc<Mutex<HashMap<String, Arc<dyn EventHandler>>>>,
    codecs: Arc<Mutex<HashMap<u16, Codec>>>,
    store: Arc<dyn GhostStore>,
    blob_manager: Arc<BlobManager>,
) {
    let kb = load_key_bundle(&store).await;

    loop {
        tokio::select! {
            Some(event) = raw_rx.recv() => {
                match event {
                    RawEvent::GossipMessage { bytes, propagation_src, topic } => {

                        // Decode envelope
                        let envelope = match GhostEnvelope::decode(bytes.as_ref()) {
                            Ok(e) => e,
                            Err(e) => {
                                tracing::warn!("Failed to decode envelope: {}", e);
                                continue;
                            }
                        };

                        // Decode message
                        let msg: GhostMessage = match postcard::from_bytes(&envelope.payload) {
                            Ok(m) => m,
                            Err(e) => {
                                tracing::warn!("Failed to deserialize message: {}", e);
                                continue;
                            }
                        };

                        let peer_id_str = propagation_src.to_string();

                        match msg.codec {

                            codec::CONTENT_TYPE_CHUNK_STORE => {
                                if let Ok(req) = postcard::from_bytes::<ChunkStoreRequest>(&msg.payload) {
                                    blob_manager.handle_store_request(req, &peer_id_str).await;
                                }
                                continue;
                            }

                            codec::CONTENT_TYPE_CHUNK_REQUEST => {
                                if let Ok(req) = postcard::from_bytes::<ChunkRequest>(&msg.payload) {
                                    blob_manager.handle_chunk_request(req, &peer_id_str).await;
                                }
                                continue;
                            }

                            codec::CONTENT_TYPE_CHUNK_RESPONSE => {
                                if let Ok(resp) = postcard::from_bytes::<ChunkResponse>(&msg.payload) {
                                    blob_manager.handle_chunk_response(resp, &peer_id_str).await;
                                }
                                continue;
                            }

                            codec::CONTENT_TYPE_BLOB_MANIFEST => {
                                if let Ok(manifest) = postcard::from_bytes::<BlobManifest>(&msg.payload) {
                                    blob_manager.handle_manifest(manifest).await;
                                }
                                continue;
                            }

                            _ => {}
                        }

                        let e = ZRPEvent::Message {
                            conversation: topic,
                            peer_id: propagation_src,
                            content_type: msg.codec,
                            payload: msg.payload.clone(),
                        };

                        let event = Arc::new(e);
                        for handler in handlers.lock().await.values() {
                            let h = handler.clone();
                            let e = event.clone();
                            let tx = crypto_tx.clone();
                            tokio::spawn(async move { h.handle(&e, tx) });
                        }
                    }

                    RawEvent::PeerDiscovered { peer_id, addr } => {
                        // Notify blob manager of new peer
                        blob_manager.on_peer_connected(peer_id.to_string()).await;

                        let e = Arc::new(ZRPEvent::PeerConnected { peer_id, addr });
                        for handler in handlers.lock().await.values() {
                            let h = handler.clone();
                            let e = e.clone();
                            let tx = crypto_tx.clone();
                            tokio::spawn(async move { h.handle(&e, tx) });
                        }
                    }

                    RawEvent::PeerLost { peer_id } => {
                        // Notify blob manager peer is gone
                        blob_manager.on_peer_disconnected(&peer_id.to_string()).await;

                        let e = Arc::new(ZRPEvent::PeerDisconnected {
                            peer_id,
                            reason: DisconnectReason::Clean,
                        });
                        for handler in handlers.lock().await.values() {
                            let h = handler.clone();
                            let e = e.clone();
                            let tx = crypto_tx.clone();
                            tokio::spawn(async move { h.handle(&e, tx) });
                        }
                    }

                    RawEvent::RelayAccepted { relay_addr } => {
                        let e = Arc::new(ZRPEvent::ConnectionStatus(
                            ConnectionStatus::Connected { relay: relay_addr }
                        ));
                        for handler in handlers.lock().await.values() {
                            let h = handler.clone();
                            let e = e.clone();
                            let tx = crypto_tx.clone();
                            tokio::spawn(async move { h.handle(&e, tx) });
                        }
                    }

                    RawEvent::RelayLost => {
                        let e = Arc::new(ZRPEvent::ConnectionStatus(
                            ConnectionStatus::Disconnected
                        ));
                        for handler in handlers.lock().await.values() {
                            let h = handler.clone();
                            let e = e.clone();
                            let tx = crypto_tx.clone();
                            tokio::spawn(async move { h.handle(&e, tx) });
                        }
                    }

                    RawEvent::PublishFailed { topic } => {
                        let e = Arc::new(ZRPEvent::MessageSendFailed {
                            conversation: topic,
                            reason: SendFailReason::NoPeersSubscribed,
                        });
                        for handler in handlers.lock().await.values() {
                            let h = handler.clone();
                            let e = e.clone();
                            let tx = crypto_tx.clone();
                            tokio::spawn(async move { h.handle(&e, tx) });
                        }
                    }
                }
            }

            Some(cmd) = crypto_rx.recv() => {
                match cmd {
                    SwarmCommand::Publish { topic, payload , content_type} => {
                        tracing::info!(
                            "crypto_task: publishing topic={} content_type=0x{:04x}",
                            topic,
                            content_type
                        );

                        if let Ok(message) = GhostMessage::new(&kb, content_type, &payload) {
                            let payload = postcard::to_allocvec(&message).unwrap();
                            let envelope = encode(&payload);
                            let mut encoded = Vec::new();
                            envelope.encode(&mut encoded).unwrap();
                            let _ = swarm_tx.send(SwarmCommand::Publish {
                                topic,
                                payload: encoded,
                                content_type
                            }).await;
                        }
                    }
                    SwarmCommand::Subscribe { topic } => {
                        let _ = swarm_tx.send(SwarmCommand::Subscribe { topic }).await;
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

pub enum SwarmCommand {
    Publish {
        topic: String,
        payload: Vec<u8>,
        content_type: u16,
    },
    Subscribe {
        topic: String,
    },
    #[allow(unused)]
    Unsubscribe {
        topic: String,
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
        topic: String,
    },
}

#[derive(Clone)]
pub struct ZRPHandle {
    cmd_tx: mpsc::Sender<SwarmCommand>,
    blob_manager: Option<Arc<BlobManager>>,
}

impl ZRPHandle {
    pub async fn publish(&self, topic: String, payload: Vec<u8>) {
        self.publish_typed(topic, payload, 0).await;
    }

    pub async fn publish_typed(&self, topic: String, payload: Vec<u8>, content_type: u16) {
        let _ = self
            .cmd_tx
            .send(SwarmCommand::Publish {
                topic,
                payload,
                content_type,
            })
            .await;
    }

    pub async fn send(
        tx: tokio::sync::mpsc::Sender<SwarmCommand>,
        topic: String,
        payload: Vec<u8>,
    ) {
        Self::send_typed(tx, topic, payload, 0).await;
    }

    pub async fn send_typed(
        tx: tokio::sync::mpsc::Sender<SwarmCommand>,
        topic: String,
        payload: Vec<u8>,
        content_type: u16,
    ) {
        let _ = tx
            .send(SwarmCommand::Publish {
                topic,
                payload,
                content_type,
            })
            .await;
    }

    pub async fn subscribe(&self, topic: String) {
        let _ = self.cmd_tx.send(SwarmCommand::Subscribe { topic }).await;
    }

    pub async fn unsubscribe(&self, topic: String) {
        let _ = self.cmd_tx.send(SwarmCommand::Unsubscribe { topic }).await;
    }

    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(SwarmCommand::Shutdown).await;
    }

    pub async fn discover_relays(
        grpc_addr: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut client = RelayClient::connect(grpc_addr).await?;
        let relays = client.list_relays().await?;

        relays
            .iter()
            .for_each(|relay| tracing::info!("{:?}", relay));

        Ok(relays.into_iter().map(|r| r.multiaddr).collect())
    }

    pub async fn upload_blob(&self, data: Vec<u8>) -> Result<String, crate::blob::BlobError> {
        self.blob_manager.as_ref().unwrap().upload(data).await
    }

    pub async fn retrieve_blob(&self, blob_id: &str) -> Result<Vec<u8>, crate::blob::BlobError> {
        self.blob_manager.as_ref().unwrap().retrieve(blob_id).await
    }

    pub fn blob_storage_bytes(&self) -> u64 {
        self.blob_manager.as_ref().unwrap().storage_usage()
    }

    pub fn evict_old_blobs(&self, max_age_secs: u64) {
        self.blob_manager.clone().unwrap().evict_old(max_age_secs);
    }
}

pub struct ZRPContext {
    codecs: Arc<Mutex<HashMap<u16, Codec>>>,
    handlers: Arc<Mutex<HashMap<String, Arc<dyn EventHandler>>>>,
    store: Arc<dyn GhostStore>,
    blob_manager: Option<Arc<BlobManager>>,
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

    pub fn blob_manager(&self) -> Option<Arc<BlobManager>> {
        self.blob_manager.clone()
    }

    pub async fn start<B, F>(
        self,
        identity: crate::identity::NodeIdentity,
        relay_addrs: Option<Vec<String>>,
        grpc_relays: Option<Vec<String>>,
        transport_config: crate::transport::TransportConfig,
        make_behavior: F,
    ) -> Result<ZRPHandle, Box<dyn std::error::Error>>
    where
        B: crate::traits::MeshBehaviour + Send + 'static,
        B::ToSwarm: Send,
        F: FnOnce(&libp2p::identity::Keypair, libp2p::relay::client::Behaviour) -> B,
    {
        let mut relays = relay_addrs.unwrap_or_default();

        if let Some(grpc) = grpc_relays {
            for grpc_addr in grpc {
                match ZRPHandle::discover_relays(&grpc_addr).await {
                    Ok(mut relay) => relays.append(&mut relay),
                    Err(e) => tracing::info!("Failed to discover relays: {}", e),
                }
            }
        }

        if relays.is_empty() {
            match ZRPHandle::discover_relays("http://relay.a.central.us.infra.zkrp.net:9001").await
            {
                Ok(mut relay) => relays.append(&mut relay),
                Err(e) => tracing::info!("Failed to discover relays: {}", e),
            }
        }

        let (crypto_tx, crypto_rx) = mpsc::channel::<SwarmCommand>(32);
        let (swarm_tx, swarm_rx) = mpsc::channel::<SwarmCommand>(32);
        let (raw_tx, raw_rx) = mpsc::channel::<RawEvent>(64);

        let store_handle = ZRPHandle {
            cmd_tx: crypto_tx.clone(),
            blob_manager: None,
        };

        let blob_manager = Arc::new(BlobManager::new(
            Arc::clone(&self.store),
            store_handle,
            identity.peer_id_string(),
        ));

        let swarm = libp2p::SwarmBuilder::with_existing_identity(identity.keypair)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_quic()
            .with_dns_config(ResolverConfig::cloudflare(), ResolverOpts::default())
            .with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)?
            .with_behaviour(|key, relay_client| make_behavior(key, relay_client))?
            .with_swarm_config(|cfg| {
                cfg.with_idle_connection_timeout(transport_config.idle_connection_timeout)
            })
            .build();

        tokio::spawn(swarm_task::<B>(
            swarm,
            relays,
            transport_config,
            swarm_rx,
            raw_tx,
            Arc::clone(&self.store),
        ));

        let handlers = Arc::clone(&self.handlers);
        let codecs = Arc::clone(&self.codecs);
        tokio::spawn(crypto_task(
            raw_rx,
            crypto_rx,
            swarm_tx,
            crypto_tx.clone(),
            handlers,
            codecs,
            Arc::clone(&self.store),
            Arc::clone(&blob_manager),
        ));

        Ok(ZRPHandle {
            cmd_tx: crypto_tx,
            blob_manager: Some(blob_manager),
        })
    }

    pub fn with_store(store: Arc<dyn GhostStore>) -> Self {
        let mut codecs = HashMap::new();
        codecs.insert(0x0001, Codec::text());
        Self {
            codecs: Arc::new(Mutex::new(codecs)),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            store,
            blob_manager: None,
        }
    }
}

impl Default for ZRPContext {
    fn default() -> Self {
        let mut codecs = HashMap::new();

        codecs.insert(0x0001, Codec::text());

        Self {
            codecs: Arc::new(Mutex::new(codecs)),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            store: Arc::new(crate::store::MemoryStore::new()),
            blob_manager: None,
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
