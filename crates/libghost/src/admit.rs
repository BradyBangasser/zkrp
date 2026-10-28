//! zkrp admission: proving to a relay that we hold a valid credential without
//! revealing anything else (see the `zkcred` crate).
//!
//! * libp2p: right after connecting to a relay, the client sends one
//!   `/zkrp/admit/1` request carrying an [`zkcred::Admission`]. Relays drop
//!   peers that haven't been admitted within a few seconds.
//! * gRPC: calls to relay services that cost the relay something (blob,
//!   mailbox, logging) carry an admission in [`GRPC_METADATA_KEY`].

use std::collections::HashMap;
use std::io;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{PeerId, StreamProtocol, request_response};

use crate::store::GhostStore;

pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/zkrp/admit/1");
/// Binary gRPC metadata (the `-bin` suffix makes tonic base64 it).
pub const GRPC_METADATA_KEY: &str = "x-zkrp-admit-bin";
/// An admission is ~400 bytes; anything much larger is not one.
const MAX_REQUEST_BYTES: u64 = 4096;

/// gRPC services that require an admission, as bound into the context.
pub const SERVICE_BLOB: &str = "zrp.relay.v1.BlobStore";
pub const SERVICE_LOGGING: &str = "zrp.relay.v1.Logging";
pub const SERVICE_MAILBOX: &str = "zrp.mailbox.v1.MailboxService";

const CREDENTIAL_KEY: &str = "zkrp/credential/v1";
const PARAMS_LABEL_KEY: &str = "zkrp/credential/params_label";
const EPOCH_SECS_KEY: &str = "zkrp/credential/epoch_secs";
/// Assumed when the issuer didn't say (matches the auth service default).
const DEFAULT_EPOCH_SECS: u64 = 2_592_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitResponse {
    Admitted,
    Denied,
}

#[derive(Clone, Default)]
pub struct AdmitCodec;

#[async_trait::async_trait]
impl request_response::Codec for AdmitCodec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = AdmitResponse;

    async fn read_request<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<Vec<u8>>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buf = Vec::new();
        io.take(MAX_REQUEST_BYTES + 1).read_to_end(&mut buf).await?;
        if buf.len() as u64 > MAX_REQUEST_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "admission too large"));
        }
        Ok(buf)
    }

    async fn read_response<T>(&mut self, _: &StreamProtocol, io: &mut T) -> io::Result<AdmitResponse>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut b = [0u8; 1];
        io.read_exact(&mut b).await?;
        Ok(if b[0] == 0 { AdmitResponse::Admitted } else { AdmitResponse::Denied })
    }

    async fn write_request<T>(&mut self, _: &StreamProtocol, io: &mut T, req: Vec<u8>) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&req).await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        _: &StreamProtocol,
        io: &mut T,
        res: AdmitResponse,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&[(res == AdmitResponse::Denied) as u8]).await?;
        io.close().await
    }
}

pub type AdmitBehaviour = request_response::Behaviour<AdmitCodec>;

fn behaviour(support: request_response::ProtocolSupport) -> AdmitBehaviour {
    request_response::Behaviour::new(
        [(PROTOCOL, support)],
        request_response::Config::default()
            .with_request_timeout(std::time::Duration::from_secs(10)),
    )
}

/// Clients only send admissions.
pub fn client_behaviour() -> AdmitBehaviour {
    behaviour(request_response::ProtocolSupport::Outbound)
}

/// Relays only receive them.
pub fn relay_behaviour() -> AdmitBehaviour {
    behaviour(request_response::ProtocolSupport::Inbound)
}

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `Params::new` hashes to the curve; do it once per label.
pub fn params(label: &str) -> zkcred::Params {
    static CACHE: OnceLock<Mutex<HashMap<String, zkcred::Params>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    cache
        .entry(label.to_string())
        .or_insert_with(|| zkcred::Params::new(label))
        .clone()
}

// --- credential storage -------------------------------------------------------

pub fn save_credential(
    store: &dyn GhostStore,
    credential: &zkcred::Credential,
    params_label: &str,
    epoch_secs: u64,
) {
    store.set(CREDENTIAL_KEY, credential.to_bytes());
    store.set(PARAMS_LABEL_KEY, params_label.as_bytes().to_vec());
    store.set(EPOCH_SECS_KEY, epoch_secs.to_be_bytes().to_vec());
}

/// Unix time after which relays stop accepting the stored credential: they
/// accept its epoch and the one after it.
pub fn credential_expires_at(store: &dyn GhostStore) -> Option<u64> {
    let (cred, _) = load_credential(store)?;
    Some((cred.epoch() + 2).saturating_mul(epoch_secs(store)))
}

/// The epoch length the stored credential was issued with.
pub fn epoch_secs(store: &dyn GhostStore) -> u64 {
    store
        .get(EPOCH_SECS_KEY)
        .and_then(|b| b.try_into().ok().map(u64::from_be_bytes))
        .filter(|s| *s > 0)
        .unwrap_or(DEFAULT_EPOCH_SECS)
}

/// The stored credential and the parameters it was issued under.
pub fn load_credential(store: &dyn GhostStore) -> Option<(zkcred::Credential, zkcred::Params)> {
    let cred = zkcred::Credential::from_bytes(&store.get(CREDENTIAL_KEY)?).ok()?;
    let label = store
        .get(PARAMS_LABEL_KEY)
        .and_then(|b| String::from_utf8(b).ok())
        .unwrap_or_else(|| zkcred::DEFAULT_PARAMS_LABEL.to_string());
    Some((cred, params(&label)))
}

/// True when a credential is stored and relays will still accept it.
pub fn has_credential(store: &dyn GhostStore) -> bool {
    credential_expires_at(store).is_some_and(|t| now_unix() < t)
}

pub fn clear_credential(store: &dyn GhostStore) {
    store.delete(CREDENTIAL_KEY);
    store.delete(PARAMS_LABEL_KEY);
    store.delete(EPOCH_SECS_KEY);
}

// --- building admissions ------------------------------------------------------

/// A libp2p admission for `relay`, or None without a credential.
pub fn p2p_admission(store: &dyn GhostStore, relay: &PeerId, me: &PeerId) -> Option<Vec<u8>> {
    let (cred, params) = load_credential(store)?;
    let (relay, me) = (relay.to_bytes(), me.to_bytes());
    let scope = zkcred::Scope::P2p { relay_peer_id: &relay, client_peer_id: &me };
    match zkcred::Admission::create(&params, &cred, scope, now_unix(), &mut zkcred::rand::rngs::OsRng) {
        Ok(a) => Some(a.to_bytes()),
        Err(e) => {
            tracing::warn!("could not build admission: {e}");
            None
        }
    }
}

/// Attach a gRPC admission for `service` (e.g. "zrp.relay.v1.BlobStore") on
/// the relay at `grpc_addr`. Without a credential the request goes out bare
/// and the relay answers UNAUTHENTICATED.
pub async fn attach_grpc_admission<T>(
    request: &mut tonic::Request<T>,
    store: &dyn GhostStore,
    grpc_addr: &str,
    service: &str,
) {
    let Some((cred, params)) = load_credential(store) else {
        tracing::warn!("no credential; {service} call will be refused");
        return;
    };
    let Some(relay) = relay_peer_id(grpc_addr).await else {
        tracing::warn!("could not learn relay peer id for {grpc_addr}");
        return;
    };
    let relay = relay.to_bytes();
    let scope = zkcred::Scope::Grpc { relay_peer_id: &relay, service };
    match zkcred::Admission::create(&params, &cred, scope, now_unix(), &mut zkcred::rand::rngs::OsRng) {
        Ok(a) => {
            request.metadata_mut().insert_bin(
                GRPC_METADATA_KEY,
                tonic::metadata::MetadataValue::from_bytes(&a.to_bytes()),
            );
        }
        Err(e) => tracing::warn!("could not build admission: {e}"),
    }
}

/// The relay's peer id, from its (unauthenticated) Health RPC, cached per
/// address for the life of the process.
async fn relay_peer_id(grpc_addr: &str) -> Option<PeerId> {
    static CACHE: OnceLock<Mutex<HashMap<String, PeerId>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(id) = cache.lock().unwrap().get(grpc_addr) {
        return Some(*id);
    }
    let mut client = crate::relay::RelayClient::connect(grpc_addr).await.ok()?;
    let id: PeerId = client.peer_id().await.ok()?.parse().ok()?;
    cache.lock().unwrap().insert(grpc_addr.to_string(), id);
    Some(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    fn credential_for_epoch(epoch: u64) -> zkcred::Credential {
        let params = params(zkcred::DEFAULT_PARAMS_LABEL);
        let sk = zkcred::IssuerSecretKey::generate(&mut zkcred::rand::rngs::OsRng);
        let (pending, req) = zkcred::request(&params, b"t", &mut zkcred::rand::rngs::OsRng);
        let sig = zkcred::issue(&params, &sk, &req, b"t", epoch, &mut zkcred::rand::rngs::OsRng).unwrap();
        zkcred::finalize(&params, pending, &sig, epoch, &sk.public_key(&params)).unwrap()
    }

    #[test]
    fn expiry_covers_the_grace_epoch() {
        let store = MemoryStore::new();
        assert!(!has_credential(&store));
        let epoch_secs = 3600;
        let now_epoch = zkcred::epoch_at(now_unix(), epoch_secs);
        save_credential(&store, &credential_for_epoch(now_epoch - 1), zkcred::DEFAULT_PARAMS_LABEL, epoch_secs);
        assert_eq!(credential_expires_at(&store), Some((now_epoch + 1) * epoch_secs));
        assert!(has_credential(&store));
        save_credential(&store, &credential_for_epoch(now_epoch - 2), zkcred::DEFAULT_PARAMS_LABEL, epoch_secs);
        assert!(!has_credential(&store), "two epochs old is no longer accepted");
    }
}
