//! Client side of anonymous enrollment against the zkrp auth service.
//!
//! 1. `request_link(email)`: the server emails a magic link; the returned
//!    challenge is kept in the store.
//! 2. The user taps the link and the app opens with the token.
//! 3. `begin(store)`: a fresh link secret and credential request, plus the
//!    clientDataHash to attest over.
//! 4. The app calls DCAppAttestService.generateKey + attestKey(keyId, clientDataHash).
//! 5. `finish(token, attestation)`: the credential is saved to the store.
//!
//! The email address is only ever sent to the auth service in step 1.

use base64::Engine;
use tonic::transport::Channel;

use crate::store::GhostStore;

pub mod proto {
    tonic::include_proto!("zrp.auth.v1");
}

use proto::auth_service_client::AuthServiceClient;
use proto::{ChallengeRequest, RedeemRequest};

const CHALLENGE_KEY: &str = "zkrp/enroll/challenge";

#[derive(Debug, thiserror::Error)]
pub enum EnrollError {
    #[error("auth service unreachable: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("{}", .0.message())]
    Rejected(#[from] tonic::Status),
    #[error("no enrollment in progress; request a new link")]
    NoChallenge,
    #[error("magic link is malformed")]
    BadToken,
    #[error("issuer key does not match the pinned key")]
    IssuerMismatch,
    #[error("credential: {0}")]
    Credential(#[from] zkcred::Error),
}

async fn client(auth_addr: &str) -> Result<AuthServiceClient<Channel>, EnrollError> {
    Ok(AuthServiceClient::new(crate::grpc::connect(auth_addr).await?))
}

/// Step 1: ask for a magic link. Returns the link's token itself when the
/// service runs without email proof (testing); redeem it as if from the link.
pub async fn request_link(
    store: &dyn GhostStore,
    auth_addr: &str,
    email: &str,
) -> Result<Option<String>, EnrollError> {
    let resp = client(auth_addr)
        .await?
        .get_challenge(ChallengeRequest {
            email: email.to_string(),
        })
        .await?
        .into_inner();
    store.set(CHALLENGE_KEY, resp.challenge);
    Ok(Some(resp.token).filter(|t| !t.is_empty()))
}

/// An enrollment between `begin` and `finish`.
pub struct Enrollment {
    params: zkcred::Params,
    pending: zkcred::PendingCredential,
    request: Vec<u8>,
}

/// Step 3: create the credential request. Returns the 32-byte clientDataHash
/// the app must pass to `attestKey`.
pub fn begin(store: &dyn GhostStore) -> Result<(Enrollment, [u8; 32]), EnrollError> {
    let challenge = store.get(CHALLENGE_KEY).ok_or(EnrollError::NoChallenge)?;
    let params = crate::admit::params(zkcred::DEFAULT_PARAMS_LABEL);
    let (pending, request) = zkcred::request(&params, &challenge, &mut zkcred::rand::rngs::OsRng);
    let client_data_hash = zkcred::attestation_binding(&challenge, &request);
    Ok((Enrollment { params, pending, request }, client_data_hash))
}

/// Step 5: redeem the magic link. `token` is the link fragment (base64url).
/// `pinned_issuer` is the issuer public key shipped with the app; when set, a
/// server that answers with any other key is refused, so it can't tag users
/// with per-user keys. Returns the credential's epoch.
pub async fn finish(
    store: &dyn GhostStore,
    auth_addr: &str,
    enrollment: Enrollment,
    token: &str,
    attestation: Vec<u8>,
    pinned_issuer: Option<&[u8]>,
) -> Result<u64, EnrollError> {
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token.trim().trim_start_matches('#'))
        .map_err(|_| EnrollError::BadToken)?;

    let resp = client(auth_addr)
        .await?
        .redeem(RedeemRequest {
            verification_token: token,
            attestation,
            credential_request: enrollment.request,
        })
        .await?
        .into_inner();

    let issuer_bytes = match pinned_issuer {
        Some(pinned) if pinned != resp.issuer_public_key.as_slice() => {
            return Err(EnrollError::IssuerMismatch);
        }
        Some(pinned) => pinned.to_vec(),
        None => resp.issuer_public_key,
    };
    if resp.params_label != enrollment.params.label() {
        return Err(zkcred::Error::Malformed("params label").into());
    }
    let issuer = zkcred::IssuerPublicKey::from_bytes(&issuer_bytes)?;
    let credential = zkcred::finalize(
        &enrollment.params,
        enrollment.pending,
        &resp.signature,
        resp.epoch,
        &issuer,
    )?;

    crate::admit::save_credential(store, &credential, enrollment.params.label(), resp.epoch_secs);
    crate::community::save(
        store,
        resp.community_keys
            .into_iter()
            .filter_map(|k| Some((k.epoch, k.key.try_into().ok()?))),
    );
    store.delete(CHALLENGE_KEY);
    Ok(resp.epoch)
}
