use crate::relay::RelayClient;
use crate::{
    codec::Codec,
    handler::{ConnectionStatus, DisconnectReason, EventHandler, SendFailReason, ZRPEvent},
    keybundle::KeyBundle,
    protocols::ghost::v0::{GhostEnvelope, GhostMessage, encode},
    store::GhostStore,
};
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

pub fn storage_usage(&self) -> u64 {
    self.store
        .list("ghost/blobs/chunks/")
        .into_iter()
        .filter_map(|k| self.store.get(&k))
        .map(|v| v.len() as u64)
        .sum()
}

pub fn evict_old(&self, max_age_secs: u64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let manifests = self.store.list("ghost/blobs/manifests/");
    let mut evicted = 0usize;

    for key in manifests {
        if let Some(bytes) = self.store.get(&key) {
            if let Ok(manifest) = postcard::from_bytes::<BlobManifest>(&bytes) {
                if now.saturating_sub(manifest.created_at) > max_age_secs {
                    for chunk_id in &manifest.chunk_ids {
                        let chunk_key = format!("ghost/blobs/chunks/{}", hex::encode(chunk_id));
                        self.store.delete(&chunk_key);
                        evicted += 1;
                    }
                    self.store.delete(&format!(
                        "ghost/blobs/cache/{}",
                        hex::encode(manifest.blob_id)
                    ));
                    self.store.delete(&key);
                }
            }
        }
    }

    if evicted > 0 {
        tracing::info!("Evicted {} old chunks from blob storage", evicted);
    }
}

struct PendingMessage {
    id: String,
    topic_hash: String,
    payload: Vec<u8>,
    attempts: u32,
    next_retry: tokio::time::Instant,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OutboxEntry {
    id: String,
    topic_hash: String,
    payload: Vec<u8>,
    attempts: u32,
}

impl From<&PendingMessage> for OutboxEntry {
    fn from(m: &PendingMessage) -> Self {
        Self {
            id: m.id.clone(),
            topic_hash: m.topic_hash.clone(),
            payload: m.payload.clone(),
            attempts: m.attempts,
        }
    }
}

pub async fn handle_store_request(&self, request: ChunkStoreRequest, from_peer: &str) {
    let chunk_id_hex = hex::encode(request.chunk.chunk_id);

    match self.decrypt_chunk(&request.chunk) {
        Ok(_plaintext) => {
            // Hash matches
        }
        Err(BlobError::HashMismatch) => {
            tracing::warn!(
                "Chunk {} from {} failed hash verification — ignoring",
                &chunk_id_hex[..8],
                &from_peer[..8]
            );
            return;
        }
        Err(e) => {
            tracing::warn!("Chunk {} decryption error: {}", &chunk_id_hex[..8], e);
            return;
        }
    }

    let usage = self.storage_usage();
    let max_bytes: u64 = 50 * 1024 * 1024; // TODO: CHANGE TO CONFIG LIMIT

    if usage > max_bytes {
        tracing::warn!(
            "Blob storage full ({} MB), rejecting chunk from {}",
            usage / 1024 / 1024,
            &from_peer[..8]
        );
        let ack = ChunkStoreAck {
            chunk_id: request.chunk.chunk_id,
            stored: false,
        };
        let topic = format!("ghost/blobs/store/{}", from_peer);
        if let Ok(payload) = postcard::to_allocvec(&ack) {
            self.handle.publish(topic, payload).await;
        }
        return;
    }

    let key = format!("ghost/blobs/chunks/{}", chunk_id_hex);
    match postcard::to_allocvec(&request.chunk) {
        Ok(bytes) => {
            self.store.set(&key, bytes);
            tracing::debug!(
                "Stored chunk {} for peer {}",
                &chunk_id_hex[..8],
                &from_peer[..8]
            );
        }
        Err(e) => {
            tracing::warn!("Failed to serialize chunk: {}", e);
            return;
        }
    }

    let ack = ChunkStoreAck {
        chunk_id: request.chunk.chunk_id,
        stored: true,
    };
    let topic = format!("ghost/blobs/store/{}", from_peer);
    if let Ok(payload) = postcard::to_allocvec(&ack) {
        self.handle.publish(topic, payload).await;
    }
}

pub async fn handle_chunk_request(&self, request: ChunkRequest, from_peer: &str) {
    let chunk_id_hex = hex::encode(request.chunk_id);
    let key = format!("ghost/blobs/chunks/{}", chunk_id_hex);

    let response = match self.store.get(&key) {
        Some(bytes) => match postcard::from_bytes::<BlobChunk>(&bytes) {
            Ok(chunk) => {
                tracing::debug!(
                    "Serving chunk {} to peer {}",
                    &chunk_id_hex[..8],
                    &from_peer[..8]
                );
                ChunkResponse { chunk, found: true }
            }
            Err(e) => {
                tracing::warn!("Failed to deserialize chunk {}: {}", &chunk_id_hex[..8], e);
                ChunkResponse {
                    chunk: BlobChunk {
                        blob_id: request.blob_id,
                        chunk_index: 0,
                        chunk_id: request.chunk_id,
                        ciphertext: vec![],
                        nonce: [0u8; 12],
                    },
                    found: false,
                }
            }
        },
        None => {
            tracing::debug!(
                "Don't have chunk {} requested by {}",
                &chunk_id_hex[..8],
                &from_peer[..8]
            );
            ChunkResponse {
                chunk: BlobChunk {
                    blob_id: request.blob_id,
                    chunk_index: 0,
                    chunk_id: request.chunk_id,
                    ciphertext: vec![],
                    nonce: [0u8; 12],
                },
                found: false,
            }
        }
    };

    let topic = format!("ghost/blobs/store/{}", from_peer);
    if let Ok(payload) = postcard::to_allocvec(&response) {
        self.handle.publish(topic, payload).await;
    }
}

pub async fn handle_chunk_response(&self, response: ChunkResponse, from_peer: &str) {
    if !response.found {
        tracing::debug!(
            "Peer {} doesn't have chunk {}",
            &from_peer[..8],
            &hex::encode(response.chunk.chunk_id)[..8]
        );
        return;
    }

    let chunk_id_hex = hex::encode(response.chunk.chunk_id);
    let blob_id_hex = hex::encode(response.chunk.blob_id);

    let plaintext = match self.decrypt_chunk(&response.chunk) {
        Ok(p) => p,
        Err(BlobError::HashMismatch) => {
            tracing::warn!(
                "Chunk {} from {} failed verification",
                &chunk_id_hex[..8],
                &from_peer[..8]
            );
            return;
        }
        Err(e) => {
            tracing::warn!("Chunk decryption error: {}", e);
            return;
        }
    };

    let store_key = format!("ghost/blobs/chunks/{}", chunk_id_hex);
    if let Ok(bytes) = postcard::to_allocvec(&response.chunk) {
        self.store.set(&store_key, bytes);
    }

    let mut retrievals = self.retrievals.lock().await;
    if let Some(state) = retrievals.get_mut(&blob_id_hex) {
        state
            .received_chunks
            .insert(response.chunk.chunk_index, plaintext);

        let received = state.received_chunks.len();
        let total = state.manifest.chunk_ids.len();

        tracing::debug!(
            "Blob {} progress: {}/{} chunks",
            &blob_id_hex[..8],
            received,
            total
        );
    }
}

pub async fn handle_manifest(&self, manifest: BlobManifest) {
    let blob_id_hex = hex::encode(manifest.blob_id);
    let key = format!("ghost/blobs/manifests/{}", blob_id_hex);

    if self.store.get(&key).is_some() {
        return;
    }

    if let Ok(bytes) = postcard::to_allocvec(&manifest) {
        self.store.set(&key, bytes);
        tracing::debug!(
            "Stored manifest for blob {} from {}",
            &blob_id_hex[..8],
            &manifest.sender_peer_id[..8]
        );
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

    let mut retry_interval = tokio::time::interval(Duration::from_millis(500));
    let mut outbox: Vec<PendingMessage> = store
        .list("ghost/outbox/")
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<OutboxEntry>(&bytes).ok())
        .map(|e| PendingMessage {
            id: e.id,
            topic_hash: e.topic_hash,
            payload: e.payload,
            attempts: e.attempts,
            next_retry: tokio::time::Instant::now(),
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

                    let topic = libp2p::gossipsub::IdentTopic::new(&msg.topic_hash);
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
                                    topic_hash: msg.topic_hash.clone(),
                                }).await;
                            }
                        }
                        Err(e) => {
                            store.delete(&format!("ghost/outbox/{}", msg.id));
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
                        let msg = PendingMessage {
                            id: Ulid::new().to_string(),
                            topic_hash,
                            payload,
                            attempts: 0,
                            next_retry: tokio::time::Instant::now(),
                        };
                        store.set(
                            &format!("ghost/outbox/{}", msg.id),
                            postcard::to_allocvec(&OutboxEntry::from(&msg)).unwrap(),
                        );
                        outbox.push(msg);
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

async fn load_key_bundle(store: &Arc<dyn GhostStore>) -> KeyBundle {
    KeyBundle::load_or_generate(store)
}

async fn crypto_task(
    mut raw_rx: mpsc::Receiver<RawEvent>,
    mut crypto_rx: mpsc::Receiver<SwarmCommand>,
    swarm_tx: mpsc::Sender<SwarmCommand>,
    handlers: Arc<Mutex<HashMap<String, Arc<dyn EventHandler>>>>,
    codecs: Arc<Mutex<HashMap<u16, Codec>>>,
    store: Arc<dyn GhostStore>,
    blob_manager: Arc<crate::blob::BlobManager>,
) {
    let kb = load_key_bundle(&store).await;

    loop {
        tokio::select! {
            Some(event) = raw_rx.recv() => {
                let e: ZRPEvent = match event {
                    RawEvent::GossipMessage { bytes, propagation_src, topic } => {
                        let envelope = match GhostEnvelope::decode(bytes.as_ref()) {
                            Ok(e) => e,
                            Err(e) => {
                                tracing::warn!("failed to decode envelope: {e}");
                                continue;
                            }
                        };
                        let msg: GhostMessage = match postcard::from_bytes(&envelope.payload) {
                            Ok(m) => m,
                            Err(e) => {
                                tracing::warn!("failed to deserialize message: {e}");
                                continue;
                            }
                        };

                        let peer_str = propagation_src.to_string();

                        match msg.codec {
                            0x0050 => {
                                if let Ok(req) = postcard::from_bytes::<crate::blob::ChunkStoreRequest>(&msg.payload) {
                                    blob_manager.handle_store_request(req, &peer_str).await;
                                }
                                continue;
                            }
                            0x0051 => {
                                continue;
                            }
                            0x0052 => {
                                if let Ok(req) = postcard::from_bytes::<crate::blob::ChunkRequest>(&msg.payload) {
                                    blob_manager.handle_chunk_request(req, &peer_str).await;
                                }
                                continue;
                            }
                            0x0053 => {
                                if let Ok(resp) = postcard::from_bytes::<crate::blob::ChunkResponse>(&msg.payload) {
                                    blob_manager.handle_chunk_response(resp, &peer_str).await;
                                }
                                continue;
                            }
                            0x0054 => {
                                if let Ok(manifest) = postcard::from_bytes::<crate::blob::BlobManifest>(&msg.payload) {
                                    blob_manager.handle_manifest(manifest).await;
                                }
                                continue;
                            }
                            _ => {}
                        }

                        ZRPEvent::Message {
                            conversation: topic,
                            peer_id: propagation_src,
                            content_type: msg.codec,
                            payload: msg.payload.clone(),
                        }
                    }

                    RawEvent::PeerDiscovered { peer_id, addr } => {
                        blob_manager.on_peer_connected(peer_id.to_string()).await;
                        ZRPEvent::PeerConnected { peer_id, addr }
                    }
                    RawEvent::PeerLost { peer_id } => {
                        blob_manager.on_peer_disconnected(&peer_id.to_string()).await;
                        ZRPEvent::PeerDisconnected {
                            peer_id,
                            reason: DisconnectReason::Clean,
                        }
                    }
                    RawEvent::RelayAccepted { relay_addr } => {
                        ZRPEvent::ConnectionStatus(ConnectionStatus::Connected { relay: relay_addr })
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
                            let msg_bytes = postcard::to_allocvec(&message).unwrap();
                            let envelope = encode(&msg_bytes);
                            let mut encoded = Vec::new();
                            envelope.encode(&mut encoded).unwrap();
                            let _ = swarm_tx.send(SwarmCommand::Publish {
                                topic_hash,
                                payload: encoded,
                            }).await;
                        }
                    }
                    SwarmCommand::Subscribe { topic_hash } => {
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
}

pub struct ZRPContext {
    codecs: Arc<Mutex<HashMap<u16, Codec>>>,
    handlers: Arc<Mutex<HashMap<String, Arc<dyn EventHandler>>>>,
    store: Arc<dyn GhostStore>,
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
            match ZRPHandle::discover_relays("http://127.0.0.1:9001").await {
                Ok(mut relay) => relays.append(&mut relay),
                Err(e) => tracing::info!("Failed to discover relays: {}", e),
            }
        }

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
            handlers,
            codecs,
            Arc::clone(&self.store),
        ));

        Ok(ZRPHandle { cmd_tx: crypto_tx })
    }

    pub fn with_store(store: Arc<dyn GhostStore>) -> Self {
        let mut codecs = HashMap::new();
        codecs.insert(0x0001, Codec::text());
        Self {
            codecs: Arc::new(Mutex::new(codecs)),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            store,
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
