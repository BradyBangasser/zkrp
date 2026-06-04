use crate::context::ZRPHandle;
use crate::store::GhostStore;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub const CHUNK_SIZE: usize = 32 * 1024;
pub const TOTAL_HOLDERS: usize = 10;
pub const MIN_HOLDERS: usize = 2;

pub type ChunkId = [u8; 32];
pub type BlobId = [u8; 32];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobManifest {
    pub blob_id: BlobId,
    pub chunk_ids: Vec<ChunkId>,
    pub chunk_holders: HashMap<String, Vec<String>>,
    pub total_size: u32,
    pub created_at: u64,
    pub sender_peer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobChunk {
    pub blob_id: BlobId,
    pub chunk_index: u32,
    pub chunk_id: ChunkId,
    pub ciphertext: Vec<u8>,
    pub nonce: [u8; 12],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkStoreRequest {
    pub chunk: BlobChunk,
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkStoreAck {
    pub chunk_id: ChunkId,
    pub stored: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRequest {
    pub blob_id: BlobId,
    pub chunk_id: ChunkId,
    pub requester_peer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkResponse {
    pub chunk: BlobChunk,
    pub found: bool,
}

// Key is derived from the blob_id itself, anyone with the blob_id can decrypt.
// The blob_id is only shared with intended recipients (in a message payload).
// The relay and chunk-holding peers never see the blob_id, they only see
// chunk_ids (hashes of ciphertext) which reveal nothing.

pub fn derive_chunk_key(blob_id: &BlobId, chunk_index: u32) -> [u8; 32] {
    let mut hasher = Sha3_256::new();
    hasher.update(blob_id);
    hasher.update(b"chunk_key");
    hasher.update(chunk_index.to_le_bytes());
    hasher.finalize().into()
}

#[derive(Debug)]
pub struct RetrievalState {
    pub blob_id: BlobId,
    pub manifest: BlobManifest,
    pub received_chunks: HashMap<u32, Vec<u8>>,
    pub requested_from: HashMap<String, Vec<String>>,
}

impl RetrievalState {
    pub fn new(manifest: BlobManifest) -> Self {
        let blob_id = manifest.blob_id;
        Self {
            blob_id,
            manifest,
            received_chunks: HashMap::new(),
            requested_from: HashMap::new(),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.received_chunks.len() == self.manifest.chunk_ids.len()
    }

    pub fn missing_chunks(&self) -> Vec<(u32, ChunkId)> {
        self.manifest
            .chunk_ids
            .iter()
            .enumerate()
            .filter(|(i, _)| !self.received_chunks.contains_key(&(*i as u32)))
            .map(|(i, id)| (i as u32, *id))
            .collect()
    }

    pub fn assemble(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }
        let mut result = Vec::with_capacity(self.manifest.total_size as usize);
        let mut indices: Vec<u32> = self.received_chunks.keys().cloned().collect();
        indices.sort();
        for i in indices {
            result.extend_from_slice(&self.received_chunks[&i]);
        }
        Some(result)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("Encryption failed: {0}")]
    Encryption(String),
    #[error("Decryption failed: {0}")]
    Decryption(String),
    #[error("Chunk verification failed — hash mismatch")]
    HashMismatch,
    #[error("Retrieval timed out")]
    Timeout,
    #[error("Not enough peers to distribute chunks")]
    InsufficientPeers,
    #[error("Blob not found")]
    NotFound,
    #[error("Store error: {0}")]
    Store(String),
}

pub struct BlobManager {
    store: Arc<dyn GhostStore>,
    handle: ZRPHandle,
    peer_id: String,
    retrievals: Arc<Mutex<HashMap<String, RetrievalState>>>,
    connected_peers: Arc<Mutex<Vec<String>>>,
}

impl BlobManager {
    pub fn new(store: Arc<dyn GhostStore>, handle: ZRPHandle, peer_id: String) -> Self {
        Self {
            store,
            handle,
            peer_id,
            retrievals: Arc::new(Mutex::new(HashMap::new())),
            connected_peers: Arc::new(Mutex::new(Vec::new())),
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

    pub async fn on_peer_connected(&self, peer_id: String) {
        self.connected_peers.lock().await.push(peer_id);
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
            if let Some(bytes) = self.store.get(&key)
                && let Ok(manifest) = postcard::from_bytes::<BlobManifest>(&bytes)
                && now.saturating_sub(manifest.created_at) > max_age_secs
            {
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

        if evicted > 0 {
            tracing::info!("Evicted {} old chunks from blob storage", evicted);
        }
    }

    pub async fn on_peer_disconnected(&self, peer_id: &str) {
        self.connected_peers.lock().await.retain(|p| p != peer_id);
    }

    pub async fn upload(&self, data: Vec<u8>) -> Result<String, BlobError> {
        let chunks: Vec<Vec<u8>> = data.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();

        tracing::debug!("Uploading {} bytes as {} chunks", data.len(), chunks.len());

        let chunk_ids: Vec<ChunkId> = chunks
            .iter()
            .map(|chunk| {
                let mut h = Sha3_256::new();
                h.update(chunk);
                h.finalize().into()
            })
            .collect();

        let blob_id: BlobId = {
            let mut h = Sha3_256::new();
            for id in &chunk_ids {
                h.update(id);
            }
            h.finalize().into()
        };

        let blob_id_hex = hex::encode(blob_id);
        tracing::info!("Blob ID: {}", blob_id_hex);

        let encrypted: Vec<BlobChunk> = chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| {
                let key_bytes = derive_chunk_key(&blob_id, i as u32);
                let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
                let cipher = Aes256Gcm::new(key);

                let mut nonce_bytes = [0u8; 12];
                rand::rng().fill_bytes(&mut nonce_bytes);
                let nonce = Nonce::from_slice(&nonce_bytes);

                let ciphertext = cipher
                    .encrypt(nonce, chunk.as_ref())
                    .map_err(|e| BlobError::Encryption(e.to_string()))?;

                Ok(BlobChunk {
                    blob_id,
                    chunk_index: i as u32,
                    chunk_id: chunk_ids[i],
                    ciphertext,
                    nonce: nonce_bytes,
                })
            })
            .collect::<Result<Vec<_>, BlobError>>()?;

        for chunk in &encrypted {
            let key = format!("ghost/blobs/chunks/{}", hex::encode(chunk.chunk_id));
            let bytes =
                postcard::to_allocvec(chunk).map_err(|e| BlobError::Store(e.to_string()))?;
            self.store.set(&key, bytes);
        }

        let peers = self.connected_peers.lock().await.clone();
        let mut chunk_holders: HashMap<String, Vec<String>> = HashMap::new();

        for id in &chunk_ids {
            chunk_holders
                .entry(hex::encode(id))
                .or_default()
                .push(self.peer_id.clone());
        }

        if peers.is_empty() {
            tracing::warn!("No peers connected — chunks stored locally only");
        } else {
            let target_peers: Vec<&String> = peers.iter().take(TOTAL_HOLDERS - 1).collect();

            for chunk in &encrypted {
                let request = ChunkStoreRequest {
                    chunk: chunk.clone(),
                    ttl_secs: 0,
                };
                let payload =
                    postcard::to_allocvec(&request).map_err(|e| BlobError::Store(e.to_string()))?;

                for peer in &target_peers {
                    let topic = format!("ghost/blobs/store/{}", peer);
                    self.handle.publish(topic, payload.clone()).await;

                    chunk_holders
                        .entry(hex::encode(chunk.chunk_id))
                        .or_default()
                        .push(peer.to_string());
                }
            }
        }

        let manifest = BlobManifest {
            blob_id,
            chunk_ids,
            chunk_holders,
            total_size: data.len() as u32,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            sender_peer_id: self.peer_id.clone(),
        };

        let manifest_bytes =
            postcard::to_allocvec(&manifest).map_err(|e| BlobError::Store(e.to_string()))?;
        self.store.set(
            &format!("ghost/blobs/manifests/{}", blob_id_hex),
            manifest_bytes,
        );

        let broadcast_payload =
            postcard::to_allocvec(&manifest).map_err(|e| BlobError::Store(e.to_string()))?;
        self.handle
            .publish("ghost/blobs/manifests".to_string(), broadcast_payload)
            .await;

        tracing::info!(
            "Uploaded blob {} ({} chunks, {} peers)",
            &blob_id_hex[..8],
            encrypted.len(),
            self.connected_peers.lock().await.len()
        );

        Ok(blob_id_hex)
    }

    pub async fn retrieve(&self, blob_id_hex: &str) -> Result<Vec<u8>, BlobError> {
        let cache_key = format!("ghost/blobs/cache/{}", blob_id_hex);
        if let Some(cached) = self.store.get(&cache_key) {
            tracing::debug!("Blob {} served from cache", &blob_id_hex[..8]);
            return Ok(cached);
        }

        let manifest = self.load_manifest(blob_id_hex)?;

        let mut state = RetrievalState::new(manifest.clone());

        for (index, chunk_id) in manifest.chunk_ids.iter().enumerate() {
            let key = format!("ghost/blobs/chunks/{}", hex::encode(chunk_id));
            if let Some(bytes) = self.store.get(&key)
                && let Ok(chunk) = postcard::from_bytes::<BlobChunk>(&bytes)
                && let Ok(plaintext) = self.decrypt_chunk(&chunk)
            {
                state.received_chunks.insert(index as u32, plaintext);
            }
        }

        if state.is_complete() {
            let data = state.assemble().ok_or(BlobError::NotFound)?;
            self.store.set(&cache_key, data.clone());
            return Ok(data);
        }

        tracing::debug!(
            "Blob {} — have {}/{} chunks locally, requesting rest from peers",
            &blob_id_hex[..8],
            state.received_chunks.len(),
            manifest.chunk_ids.len()
        );

        {
            let mut retrievals = self.retrievals.lock().await;
            retrievals.insert(blob_id_hex.to_string(), state);
        }

        self.request_missing_chunks(blob_id_hex, &manifest).await?;

        let timeout = tokio::time::Duration::from_secs(30);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            let retrievals = self.retrievals.lock().await;
            if let Some(state) = retrievals.get(blob_id_hex) {
                if state.is_complete() {
                    let data = state.assemble().ok_or(BlobError::NotFound)?;
                    drop(retrievals);

                    self.store.set(&cache_key, data.clone());

                    self.retrievals.lock().await.remove(blob_id_hex);

                    tracing::info!(
                        "Blob {} retrieved successfully ({} bytes)",
                        &blob_id_hex[..8],
                        data.len()
                    );

                    return Ok(data);
                }

                let missing = state.missing_chunks();
                drop(retrievals);

                if tokio::time::Instant::now() > deadline {
                    self.retrievals.lock().await.remove(blob_id_hex);
                    tracing::warn!(
                        "Blob {} retrieval timed out — missing {}/{} chunks",
                        &blob_id_hex[..8],
                        missing.len(),
                        manifest.chunk_ids.len()
                    );
                    return Err(BlobError::Timeout);
                }

                if tokio::time::Instant::now()
                    .elapsed()
                    .as_secs()
                    .is_multiple_of(5)
                {
                    self.request_missing_chunks(blob_id_hex, &manifest).await?;
                }
            } else {
                break;
            }
        }

        Err(BlobError::NotFound)
    }

    fn load_manifest(&self, blob_id_hex: &str) -> Result<BlobManifest, BlobError> {
        let key = format!("ghost/blobs/manifests/{}", blob_id_hex);
        let bytes = self.store.get(&key).ok_or(BlobError::NotFound)?;
        postcard::from_bytes::<BlobManifest>(&bytes).map_err(|e| BlobError::Store(e.to_string()))
    }

    fn decrypt_chunk(&self, chunk: &BlobChunk) -> Result<Vec<u8>, BlobError> {
        let key_bytes = derive_chunk_key(&chunk.blob_id, chunk.chunk_index);
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&chunk.nonce);

        let plaintext = cipher
            .decrypt(nonce, chunk.ciphertext.as_ref())
            .map_err(|e| BlobError::Decryption(e.to_string()))?;

        let mut h = Sha3_256::new();
        h.update(&plaintext);
        let hash: ChunkId = h.finalize().into();

        if hash != chunk.chunk_id {
            return Err(BlobError::HashMismatch);
        }

        Ok(plaintext)
    }

    async fn request_missing_chunks(
        &self,
        blob_id_hex: &str,
        manifest: &BlobManifest,
    ) -> Result<(), BlobError> {
        let retrievals = self.retrievals.lock().await;
        let state = retrievals.get(blob_id_hex).ok_or(BlobError::NotFound)?;
        let missing = state.missing_chunks();
        drop(retrievals);

        for (_, chunk_id) in missing {
            let chunk_id_hex = hex::encode(chunk_id);

            let holders = manifest
                .chunk_holders
                .get(&chunk_id_hex)
                .cloned()
                .unwrap_or_default();

            if holders.is_empty() {
                tracing::warn!("No known holders for chunk {}", &chunk_id_hex[..8]);
                continue;
            }

            for peer_id in holders {
                if peer_id == self.peer_id {
                    continue;
                }

                let request = ChunkRequest {
                    blob_id: manifest.blob_id,
                    chunk_id,
                    requester_peer_id: self.peer_id.clone(),
                };

                let payload =
                    postcard::to_allocvec(&request).map_err(|e| BlobError::Store(e.to_string()))?;

                let topic = format!("ghost/blobs/store/{}", peer_id);
                self.handle.publish(topic, payload).await;

                tracing::debug!(
                    "Requested chunk {} from peer {}",
                    &chunk_id_hex[..8],
                    &peer_id[..8]
                );
            }
        }

        Ok(())
    }
}
