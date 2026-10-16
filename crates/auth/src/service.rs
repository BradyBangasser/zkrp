use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use tonic::{Request, Response, Status};

use crate::attest::AppAttest;
use crate::mailer::Mailer;
use crate::nullifier::NullifierSet;
use crate::proto::auth_service_server::AuthService;
use crate::proto::{ChallengeRequest, ChallengeResponse, RedeemRequest, RedeemResponse};
use crate::token::{CHALLENGE_LEN, TokenError, TokenKey};

/// How long a magic link stays redeemable.
pub const TOKEN_TTL: Duration = Duration::from_secs(15 * 60);
/// Magic links per address per window, so the service can't be used to
/// mail-bomb a student.
const SENDS_PER_WINDOW: u32 = 3;
const SEND_WINDOW: Duration = Duration::from_secs(15 * 60);

pub struct Auth {
    pub mailer: Arc<dyn Mailer>,
    /// Lowercase, no leading dot: ["umn.edu", "iastate.edu"]
    pub allowed_domains: Vec<String>,
    pub token_key: TokenKey,
    pub attest: AppAttest,
    pub nullifiers: NullifierSet,
    pub params: zkcred::Params,
    pub issuer: Arc<zkcred::IssuerSecretKey>,
    pub issuer_public: Vec<u8>,
    pub epoch_secs: u64,
    pub limiter: SendLimiter,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Lowercase, trim, and drop a `+tag` from the local part so one inbox can't
/// enroll under many aliases. Returns None for anything that isn't a plain
/// `local@domain`.
pub fn normalize_email(raw: &str) -> Option<String> {
    let lower = raw.trim().to_lowercase();
    let (local, domain) = lower.rsplit_once('@')?;
    let local = local.split('+').next().unwrap_or("");
    let ok_local = !local.is_empty()
        && local
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
    let ok_domain = domain.contains('.')
        && domain
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-".contains(&b));
    (ok_local && ok_domain).then(|| format!("{local}@{domain}"))
}

impl Auth {
    fn domain_allowed(&self, normalized: &str) -> bool {
        let Some((_, domain)) = normalized.rsplit_once('@') else {
            return false;
        };
        self.allowed_domains
            .iter()
            .any(|d| domain == d || domain.ends_with(&format!(".{d}")))
    }
}

/// Outcome label for `zkrp_auth_*_total{result}`: the status code name, or
/// "ok". Never anything about the address.
fn outcome<T>(r: &Result<T, Status>) -> &'static str {
    match r {
        Ok(_) => "ok",
        Err(s) => match s.code() {
            tonic::Code::InvalidArgument => "invalid",
            tonic::Code::ResourceExhausted => "rate_limited",
            tonic::Code::Unavailable => "unavailable",
            tonic::Code::DeadlineExceeded => "expired",
            tonic::Code::PermissionDenied => "attestation_failed",
            tonic::Code::AlreadyExists => "slots_exhausted",
            _ => "error",
        },
    }
}

#[tonic::async_trait]
impl AuthService for Auth {
    async fn get_challenge(
        &self,
        req: Request<ChallengeRequest>,
    ) -> Result<Response<ChallengeResponse>, Status> {
        let r = self.get_challenge_inner(req).await;
        telemetry::counter!("zkrp_auth_links_total", "result" => outcome(&r)).increment(1);
        r
    }

    async fn redeem(&self, req: Request<RedeemRequest>) -> Result<Response<RedeemResponse>, Status> {
        let r = self.redeem_inner(req).await;
        telemetry::counter!("zkrp_auth_enrollments_total", "result" => outcome(&r)).increment(1);
        r
    }
}

impl Auth {
    async fn get_challenge_inner(
        &self,
        req: Request<ChallengeRequest>,
    ) -> Result<Response<ChallengeResponse>, Status> {
        let email = normalize_email(&req.into_inner().email)
            .ok_or_else(|| Status::invalid_argument("invalid email address"))?;
        if !self.domain_allowed(&email) {
            return Err(Status::invalid_argument("this school isn't supported yet"));
        }
        if !self.limiter.allow(&email) {
            return Err(Status::resource_exhausted("too many links sent; try again later"));
        }

        let mut challenge = [0u8; CHALLENGE_LEN];
        getrandom::fill(&mut challenge).map_err(|_| Status::internal("rng unavailable"))?;
        let token = self
            .token_key
            .seal(&email, &challenge, now_unix() + TOKEN_TTL.as_secs());
        let token_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&token);

        self.mailer.send_link(&email, &token_b64).await.map_err(|e| {
            // Never log the address.
            tracing::error!("failed to send magic link: {e}");
            Status::unavailable("could not send email")
        })?;

        Ok(Response::new(ChallengeResponse {
            challenge: challenge.to_vec(),
        }))
    }

    async fn redeem_inner(
        &self,
        req: Request<RedeemRequest>,
    ) -> Result<Response<RedeemResponse>, Status> {
        let req = req.into_inner();

        let opened = self
            .token_key
            .open(&req.verification_token, now_unix())
            .map_err(|e| match e {
                TokenError::Expired => Status::deadline_exceeded("link expired; request a new one"),
                TokenError::Invalid => Status::invalid_argument("invalid link"),
            })?;

        let binding = zkcred::attestation_binding(&opened.challenge, &req.credential_request);
        self.attest
            .verify(&req.attestation, &binding)
            .map_err(|e| Status::permission_denied(format!("device attestation failed: {e}")))?;

        // Sign before claiming the nullifier so a malformed request can't burn
        // a slot; the signature is only released once the claim succeeds.
        let epoch = zkcred::epoch_at(now_unix(), self.epoch_secs);
        let (params, issuer, request, context) = (
            self.params.clone(),
            self.issuer.clone(),
            req.credential_request,
            opened.challenge,
        );
        let signature = tokio::task::spawn_blocking(move || {
            let mut rng = zkcred::rand::rngs::OsRng;
            zkcred::issue(&params, &issuer, &request, &context, epoch, &mut rng)
        })
        .await
        .map_err(|_| Status::internal("issuance task failed"))?
        .map_err(|e| Status::invalid_argument(format!("credential request: {e}")))?;

        let claimed = self.nullifiers.claim(&opened.email, epoch).await.map_err(|e| {
            tracing::error!("nullifier store: {e}");
            Status::unavailable("enrollment temporarily unavailable")
        })?;
        if !claimed {
            return Err(Status::already_exists(
                "this address has reached its device limit for now",
            ));
        }

        Ok(Response::new(RedeemResponse {
            signature,
            issuer_public_key: self.issuer_public.clone(),
            params_label: self.params.label().to_string(),
            epoch,
            epoch_secs: self.epoch_secs,
        }))
    }
}

/// Per-address send limit, in memory only. Addresses are stored as a keyed
/// hash under a key generated at startup and never persisted, and entries
/// expire after one window: the process never holds a usable list of who
/// asked for a link.
pub struct SendLimiter {
    key: ring::hmac::Key,
    seen: Mutex<HashMap<[u8; 32], (Instant, u32)>>,
}

impl SendLimiter {
    pub fn new() -> Self {
        let mut k = [0u8; 32];
        getrandom::fill(&mut k).expect("OS RNG");
        Self {
            key: ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &k),
            seen: Mutex::new(HashMap::new()),
        }
    }

    pub fn allow(&self, email: &str) -> bool {
        let tag: [u8; 32] = ring::hmac::sign(&self.key, email.as_bytes())
            .as_ref()
            .try_into()
            .unwrap();
        let now = Instant::now();
        let mut seen = self.seen.lock().unwrap();
        seen.retain(|_, (start, _)| now.duration_since(*start) < SEND_WINDOW);
        let entry = seen.entry(tag).or_insert((now, 0));
        entry.1 += 1;
        entry.1 <= SENDS_PER_WINDOW
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_and_strips_plus_aliases() {
        assert_eq!(normalize_email("  Jo.Doe+dating@IAState.edu "), Some("jo.doe@iastate.edu".into()));
        assert_eq!(normalize_email("nope"), None);
        assert_eq!(normalize_email("+tag@iastate.edu"), None);
        assert_eq!(normalize_email("a@localhost"), None);
        assert_eq!(normalize_email("a b@iastate.edu"), None);
    }

    #[test]
    fn limiter_caps_per_address() {
        let l = SendLimiter::new();
        for _ in 0..SENDS_PER_WINDOW {
            assert!(l.allow("a@x.edu"));
        }
        assert!(!l.allow("a@x.edu"));
        assert!(l.allow("b@x.edu"));
    }
}
