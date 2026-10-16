//! Relay-side verification of zkrp admissions (see `libghost::admit`).
//!
//! A relay learns exactly one thing from an admission: "this connection holds a
//! credential for an accepted epoch". It keeps no record of it beyond the
//! connection's lifetime, and its replay cache holds only random nonces.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tonic::{Request, Status};

/// Allowed clock skew between client and relay.
const MAX_SKEW_SECS: u64 = 120;
/// Nonces are remembered long enough to cover the whole skew window.
const REPLAY_TTL: Duration = Duration::from_secs(2 * MAX_SKEW_SECS + 10);

pub struct Admission {
    params: zkcred::Params,
    issuer: zkcred::IssuerPublicKey,
    epoch_secs: u64,
    relay_peer_id: Vec<u8>,
    seen: Mutex<HashMap<[u8; zkcred::ADMIT_NONCE_LEN], Instant>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Refused {
    #[error("missing admission")]
    Missing,
    #[error("malformed admission")]
    Malformed,
    #[error("clock skew too large")]
    Skew,
    #[error("credential expired or not yet valid")]
    Epoch,
    #[error("invalid credential")]
    Invalid,
    #[error("admission replayed")]
    Replay,
}

impl Refused {
    fn label(&self) -> &'static str {
        match self {
            Refused::Missing => "missing",
            Refused::Malformed => "malformed",
            Refused::Skew => "skew",
            Refused::Epoch => "epoch",
            Refused::Invalid => "invalid",
            Refused::Replay => "replay",
        }
    }
}

/// `zkrp_admissions_total{transport, result}`.
pub fn record(transport: &'static str, result: &Result<(), Refused>) {
    let result = match result {
        Ok(()) => "admitted",
        Err(e) => e.label(),
    };
    telemetry::counter!("zkrp_admissions_total", "transport" => transport, "result" => result)
        .increment(1);
}

impl Admission {
    pub fn new(
        params_label: &str,
        issuer_public_key: &[u8],
        epoch_secs: u64,
        relay_peer_id: &libp2p::PeerId,
    ) -> Result<Self, zkcred::Error> {
        Ok(Self {
            params: zkcred::Params::new(params_label),
            issuer: zkcred::IssuerPublicKey::from_bytes(issuer_public_key)?,
            epoch_secs,
            relay_peer_id: relay_peer_id.to_bytes(),
            seen: Mutex::new(HashMap::new()),
        })
    }

    pub fn relay_peer_id(&self) -> &[u8] {
        &self.relay_peer_id
    }

    /// Verify `bytes` for `scope`. CPU-bound (a pairing check, a few ms), so
    /// callers on the async runtime should run it on a blocking thread.
    pub fn check(&self, scope: zkcred::Scope<'_>, bytes: &[u8]) -> Result<(), Refused> {
        let a = zkcred::Admission::from_bytes(bytes).map_err(|_| Refused::Malformed)?;

        let now = libghost::admit::now_unix();
        if a.unix_secs.abs_diff(now) > MAX_SKEW_SECS {
            return Err(Refused::Skew);
        }

        // The current epoch, plus the previous one so credentials don't all
        // stop working at the same instant at every rollover.
        let current = zkcred::epoch_at(now, self.epoch_secs);
        let accept = |e: u64| e == current || e + 1 == current;
        let ctx = zkcred::admission_context(scope, &a.nonce, a.unix_secs);
        zkcred::verify(&self.params, &self.issuer, &a.presentation, &ctx, accept).map_err(|e| {
            match e {
                zkcred::Error::Epoch(_) => Refused::Epoch,
                zkcred::Error::Malformed(_) | zkcred::Error::Version(_) => Refused::Malformed,
                _ => Refused::Invalid,
            }
        })?;

        // Only verified nonces enter the cache, so junk can't fill it.
        let mut seen = self.seen.lock().unwrap();
        let t = Instant::now();
        if seen.len() > 4096 {
            seen.retain(|_, at| t.duration_since(*at) < REPLAY_TTL);
        }
        match seen.get(&a.nonce) {
            Some(at) if t.duration_since(*at) < REPLAY_TTL => Err(Refused::Replay),
            _ => {
                seen.insert(a.nonce, t);
                Ok(())
            }
        }
    }

    /// gRPC interceptor requiring an admission bound to `service`. `None`
    /// admission (ADMISSION=off) lets everything through.
    pub fn interceptor(
        admission: Option<std::sync::Arc<Self>>,
        service: &'static str,
    ) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
        move |req: Request<()>| {
            let Some(adm) = &admission else {
                return Ok(req);
            };
            let result = req
                .metadata()
                .get_bin(libghost::admit::GRPC_METADATA_KEY)
                .ok_or(Refused::Missing)
                .and_then(|v| v.to_bytes().map_err(|_| Refused::Malformed))
                .and_then(|bytes| {
                    let scope = zkcred::Scope::Grpc {
                        relay_peer_id: adm.relay_peer_id(),
                        service,
                    };
                    // block_in_place keeps the pairing check off the async
                    // workers' fast path without needing an async interceptor.
                    tokio::task::block_in_place(|| adm.check(scope, &bytes))
                });
            record("grpc", &result);
            result.map_err(|e| Status::unauthenticated(e.to_string()))?;
            Ok(req)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zkcred::rand::rngs::OsRng;

    struct Fixture {
        adm: Admission,
        params: zkcred::Params,
        cred: zkcred::Credential,
        relay: Vec<u8>,
    }

    fn fixture(epoch_offset: u64) -> Fixture {
        let label = zkcred::DEFAULT_PARAMS_LABEL;
        let params = zkcred::Params::new(label);
        let sk = zkcred::IssuerSecretKey::generate(&mut OsRng);
        let pk = sk.public_key(&params);
        let epoch_secs = 3600;
        let epoch = zkcred::epoch_at(libghost::admit::now_unix(), epoch_secs) - epoch_offset;
        let (pending, req) = zkcred::request(&params, b"c", &mut OsRng);
        let sig = zkcred::issue(&params, &sk, &req, b"c", epoch, &mut OsRng).unwrap();
        let cred = zkcred::finalize(&params, pending, &sig, epoch, &pk).unwrap();
        let relay_id = libp2p::PeerId::random();
        Fixture {
            adm: Admission::new(label, &pk.to_bytes(), epoch_secs, &relay_id).unwrap(),
            params,
            cred,
            relay: relay_id.to_bytes(),
        }
    }

    fn grpc<'a>(f: &'a Fixture, service: &'a str) -> zkcred::Scope<'a> {
        zkcred::Scope::Grpc { relay_peer_id: &f.relay, service }
    }

    fn make(f: &Fixture, scope: zkcred::Scope<'_>, ts: u64) -> Vec<u8> {
        zkcred::Admission::create(&f.params, &f.cred, scope, ts, &mut OsRng)
            .unwrap()
            .to_bytes()
    }

    #[test]
    fn accepts_then_rejects_replay() {
        let f = fixture(0);
        let a = make(&f, grpc(&f, "svc"), libghost::admit::now_unix());
        f.adm.check(grpc(&f, "svc"), &a).unwrap();
        assert!(matches!(f.adm.check(grpc(&f, "svc"), &a), Err(Refused::Replay)));
    }

    #[test]
    fn accepts_previous_epoch_rejects_older() {
        let f = fixture(1);
        let a = make(&f, grpc(&f, "svc"), libghost::admit::now_unix());
        f.adm.check(grpc(&f, "svc"), &a).unwrap();
        let f = fixture(2);
        let a = make(&f, grpc(&f, "svc"), libghost::admit::now_unix());
        assert!(matches!(f.adm.check(grpc(&f, "svc"), &a), Err(Refused::Epoch)));
    }

    #[test]
    fn rejects_other_service_and_skew() {
        let f = fixture(0);
        let now = libghost::admit::now_unix();
        let a = make(&f, grpc(&f, "blob"), now);
        assert!(matches!(f.adm.check(grpc(&f, "mailbox"), &a), Err(Refused::Invalid)));
        let old = make(&f, grpc(&f, "blob"), now - MAX_SKEW_SECS - 5);
        assert!(matches!(f.adm.check(grpc(&f, "blob"), &old), Err(Refused::Skew)));
    }

    #[test]
    fn p2p_scope_is_bound_to_client_peer() {
        let f = fixture(0);
        let me = libp2p::PeerId::random().to_bytes();
        let other = libp2p::PeerId::random().to_bytes();
        let scope = zkcred::Scope::P2p { relay_peer_id: &f.relay, client_peer_id: &me };
        let a = make(&f, scope, libghost::admit::now_unix());
        let wrong = zkcred::Scope::P2p { relay_peer_id: &f.relay, client_peer_id: &other };
        assert!(f.adm.check(wrong, &a).is_err());
        f.adm.check(scope, &a).unwrap();
    }
}
