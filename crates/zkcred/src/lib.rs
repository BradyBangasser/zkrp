//! Anonymous credentials for zkrp.
//!
//! A credential is a BBS signature (2023 variant, BLS12-381) over two messages:
//!
//!   0. `link_secret` — random, chosen by the holder, never seen by the issuer
//!      (it is signed blind, under a commitment).
//!   1. `epoch`       — chosen by the issuer, revealed in every presentation.
//!
//! A presentation is a zero-knowledge proof of knowledge of such a signature
//! that reveals only the epoch, bound to a caller-supplied `context` (e.g. the
//! relay's peer id and a nonce) so it can't be replayed elsewhere. Every
//! presentation is freshly randomized: two presentations from the same
//! credential, and the issuance itself, are mutually unlinkable.
//!
//! Relays verify presentations offline against one issuer public key. That key
//! is published, so an issuer can't hand individual users their own key to
//! tag them.

use std::collections::BTreeMap;

use ark_bls12_381::{Bls12_381, Fr, G1Affine};
use ark_ec::{AffineRepr, CurveGroup};
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};
use ark_std::UniformRand;
use ark_std::rand::{CryptoRng, RngCore};
use bbs_plus::proof_23::{PoKOfSignature23G1Proof, PoKOfSignature23G1Protocol};
use bbs_plus::setup::{
    PreparedPublicKeyG2, PreparedSignatureParams23G1, PublicKeyG2, SecretKey,
    SignatureParams23G1,
};
use bbs_plus::signature_23::Signature23G1;
use blake2::Blake2b512;
use dock_crypto_utils::signature::MessageOrBlinding;
use schnorr_pok::compute_random_oracle_challenge;
use schnorr_pok::discrete_log::{PokDiscreteLog, PokDiscreteLogProtocol};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub use ark_std::rand;

const MESSAGE_COUNT: u32 = 2;
const IDX_SECRET: usize = 0;
const IDX_EPOCH: usize = 1;

/// Wire-format version byte prefixed to every serialized object.
const VERSION: u8 = 1;

const REQUEST_DOMAIN: &[u8] = b"zkrp/cred-request/v1";
const PRESENT_DOMAIN: &[u8] = b"zkrp/present/v1";

/// Default label for the public parameters. Changing it invalidates every
/// credential, so it is versioned and only ever bumped deliberately.
pub const DEFAULT_PARAMS_LABEL: &str = "zkrp/bbs23/v1";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("malformed {0}")]
    Malformed(&'static str),
    #[error("unsupported version {0}")]
    Version(u8),
    #[error("proof of commitment opening is invalid")]
    BadRequest,
    #[error("signature does not verify")]
    BadSignature,
    #[error("presentation does not verify")]
    BadPresentation,
    #[error("epoch {0} not accepted")]
    Epoch(u64),
    #[error("crypto: {0}")]
    Crypto(String),
}

impl From<bbs_plus::error::BBSPlusError> for Error {
    fn from(e: bbs_plus::error::BBSPlusError) -> Self {
        Error::Crypto(format!("{e:?}"))
    }
}

/// The epoch containing `unix_secs`.
pub fn epoch_at(unix_secs: u64, epoch_secs: u64) -> u64 {
    unix_secs / epoch_secs.max(1)
}

/// The shared key of one community (an allowed email domain) for one epoch,
/// from the issuer's community secret. Auth gives each enrollee the keys for
/// the epochs their credential is accepted in; members seal what only their
/// community should read with them. Whoever holds `secret` can derive every
/// community's keys.
pub fn community_key(secret: &[u8], community: &str, epoch: u64) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    let mut m = <Hmac<sha2::Sha256> as Mac>::new_from_slice(secret).expect("hmac key");
    m.update(b"zkrp/community/v1");
    m.update(&(community.len() as u32).to_be_bytes());
    m.update(community.as_bytes());
    m.update(&epoch.to_be_bytes());
    m.finalize().into_bytes().into()
}

fn epoch_fr(epoch: u64) -> Fr {
    Fr::from(epoch)
}

fn write<T: CanonicalSerialize>(v: &T, out: &mut Vec<u8>) {
    v.serialize_compressed(out).expect("serialize into Vec");
}

fn read_versioned<'a>(bytes: &'a [u8], what: &'static str) -> Result<&'a [u8], Error> {
    match bytes.split_first() {
        Some((&VERSION, rest)) => Ok(rest),
        Some((&v, _)) => Err(Error::Version(v)),
        None => Err(Error::Malformed(what)),
    }
}

fn read<T: CanonicalDeserialize>(r: &mut &[u8], what: &'static str) -> Result<T, Error> {
    T::deserialize_compressed(r).map_err(|_| Error::Malformed(what))
}

fn read_u64(r: &mut &[u8], what: &'static str) -> Result<u64, Error> {
    if r.len() < 8 {
        return Err(Error::Malformed(what));
    }
    let (head, tail) = r.split_at(8);
    *r = tail;
    Ok(u64::from_be_bytes(head.try_into().unwrap()))
}

/// Public parameters, derived deterministically from a label so every party
/// can reconstruct them without shipping them around.
#[derive(Clone)]
pub struct Params {
    label: String,
    sig: SignatureParams23G1<Bls12_381>,
    prepared: PreparedSignatureParams23G1<Bls12_381>,
}

impl Params {
    pub fn new(label: &str) -> Self {
        let sig = SignatureParams23G1::<Bls12_381>::new::<Blake2b512>(label.as_bytes(), MESSAGE_COUNT);
        Self {
            label: label.to_string(),
            prepared: sig.clone().into(),
            sig,
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    /// Base for the link-secret commitment.
    fn h_secret(&self) -> G1Affine {
        self.sig.h[IDX_SECRET]
    }
}

/// The App Attest `clientDataHash` for an enrollment:
/// SHA-256("zkrp/attest/v1" || challenge || SHA-256(credential_request)).
/// Attesting over this binds the attested device to this exact request.
pub fn attestation_binding(challenge: &[u8], credential_request: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"zkrp/attest/v1");
    h.update(challenge);
    h.update(Sha256::digest(credential_request));
    h.finalize().into()
}

/// Length of the random nonce in an admission.
pub const ADMIT_NONCE_LEN: usize = 16;

/// Where a presentation is being used. Part of the signed context, so a
/// presentation made for one cannot be used for the other.
#[derive(Clone, Copy)]
pub enum Scope<'a> {
    /// Admission to a relay over libp2p (`/zkrp/admit/1`).
    P2p { relay_peer_id: &'a [u8], client_peer_id: &'a [u8] },
    /// A gRPC call to one service on one relay.
    Grpc { relay_peer_id: &'a [u8], service: &'a str },
}

/// The context string a presentation is bound to. Clients and relays must
/// build it identically, so both use this.
pub fn admission_context(scope: Scope<'_>, nonce: &[u8; ADMIT_NONCE_LEN], unix_secs: u64) -> Vec<u8> {
    fn field(out: &mut Vec<u8>, b: &[u8]) {
        out.extend_from_slice(&(b.len() as u32).to_be_bytes());
        out.extend_from_slice(b);
    }
    let mut out = b"zkrp/admit/v1".to_vec();
    match scope {
        Scope::P2p { relay_peer_id, client_peer_id } => {
            out.push(1);
            field(&mut out, relay_peer_id);
            field(&mut out, client_peer_id);
        }
        Scope::Grpc { relay_peer_id, service } => {
            out.push(2);
            field(&mut out, relay_peer_id);
            field(&mut out, service.as_bytes());
        }
    }
    out.extend_from_slice(nonce);
    out.extend_from_slice(&unix_secs.to_be_bytes());
    out
}

/// An admission on the wire: nonce || unix_secs || presentation.
pub struct Admission {
    pub nonce: [u8; ADMIT_NONCE_LEN],
    pub unix_secs: u64,
    pub presentation: Vec<u8>,
}

impl Admission {
    /// Holder: build an admission for `scope` at time `unix_secs`.
    pub fn create<R: RngCore + CryptoRng>(
        params: &Params,
        credential: &Credential,
        scope: Scope<'_>,
        unix_secs: u64,
        rng: &mut R,
    ) -> Result<Self, Error> {
        let mut nonce = [0u8; ADMIT_NONCE_LEN];
        rng.fill_bytes(&mut nonce);
        let ctx = admission_context(scope, &nonce, unix_secs);
        Ok(Self {
            nonce,
            unix_secs,
            presentation: present(params, credential, &ctx, rng)?,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = self.nonce.to_vec();
        out.extend_from_slice(&self.unix_secs.to_be_bytes());
        out.extend_from_slice(&self.presentation);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < ADMIT_NONCE_LEN + 8 {
            return Err(Error::Malformed("admission"));
        }
        let (nonce, rest) = bytes.split_at(ADMIT_NONCE_LEN);
        let (ts, presentation) = rest.split_at(8);
        Ok(Self {
            nonce: nonce.try_into().unwrap(),
            unix_secs: u64::from_be_bytes(ts.try_into().unwrap()),
            presentation: presentation.to_vec(),
        })
    }
}

// --- issuer keys ------------------------------------------------------------

#[derive(Clone)]
pub struct IssuerPublicKey {
    pk: PublicKeyG2<Bls12_381>,
    prepared: PreparedPublicKeyG2<Bls12_381>,
}

impl IssuerPublicKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![VERSION];
        write(&self.pk, &mut out);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = read_versioned(bytes, "issuer public key")?;
        let pk: PublicKeyG2<Bls12_381> = read(&mut r, "issuer public key")?;
        if !pk.is_valid() {
            return Err(Error::Malformed("issuer public key"));
        }
        Ok(Self {
            prepared: pk.clone().into(),
            pk,
        })
    }
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct IssuerSecretKey(SecretKey<Fr>);

impl IssuerSecretKey {
    pub fn generate<R: RngCore + CryptoRng>(rng: &mut R) -> Self {
        Self(SecretKey(Fr::rand(rng)))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![VERSION];
        write(&self.0.0, &mut out);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = read_versioned(bytes, "issuer secret key")?;
        Ok(Self(SecretKey(read(&mut r, "issuer secret key")?)))
    }

    pub fn public_key(&self, params: &Params) -> IssuerPublicKey {
        let pk = PublicKeyG2::generate_using_secret_key_and_bbs23_params(&self.0, &params.sig);
        IssuerPublicKey {
            prepared: pk.clone().into(),
            pk,
        }
    }
}

// --- issuance -----------------------------------------------------------------

/// Holder-side state between `request` and `finalize`.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct PendingCredential {
    link_secret: Fr,
}

fn request_challenge(params: &Params, commitment: &G1Affine, t: &[u8], context: &[u8]) -> Fr {
    let mut bytes = REQUEST_DOMAIN.to_vec();
    bytes.extend_from_slice(params.label.as_bytes());
    bytes.extend_from_slice(&(context.len() as u64).to_be_bytes());
    bytes.extend_from_slice(context);
    write(commitment, &mut bytes);
    bytes.extend_from_slice(t);
    compute_random_oracle_challenge::<Fr, Blake2b512>(&bytes)
}

/// Holder: commit to a fresh link secret and prove knowledge of it. `context`
/// binds the request to one issuance session (the auth server's challenge).
pub fn request<R: RngCore + CryptoRng>(
    params: &Params,
    context: &[u8],
    rng: &mut R,
) -> (PendingCredential, Vec<u8>) {
    let link_secret = Fr::rand(rng);
    let base = params.h_secret();
    let commitment = (base * link_secret).into_affine();

    let protocol = PokDiscreteLogProtocol::init(link_secret, Fr::rand(rng), &base);
    let mut t = Vec::new();
    protocol
        .challenge_contribution(&base, &commitment, &mut t)
        .expect("serialize into Vec");
    let challenge = request_challenge(params, &commitment, &t, context);
    let pok = protocol.gen_proof(&challenge);

    let mut out = vec![VERSION];
    write(&commitment, &mut out);
    write(&pok, &mut out);
    (PendingCredential { link_secret }, out)
}

/// Issuer: check the holder knows the committed link secret, then blind-sign
/// it together with `epoch`. Returns the serialized signature.
pub fn issue<R: RngCore + CryptoRng>(
    params: &Params,
    key: &IssuerSecretKey,
    request: &[u8],
    context: &[u8],
    epoch: u64,
    rng: &mut R,
) -> Result<Vec<u8>, Error> {
    let mut r = read_versioned(request, "credential request")?;
    let commitment: G1Affine = read(&mut r, "credential request")?;
    let pok: PokDiscreteLog<G1Affine> = read(&mut r, "credential request")?;
    if !r.is_empty() || commitment.is_zero() {
        return Err(Error::Malformed("credential request"));
    }

    let base = params.h_secret();
    let mut t = Vec::new();
    pok.challenge_contribution(&base, &commitment, &mut t)
        .expect("serialize into Vec");
    let challenge = request_challenge(params, &commitment, &t, context);
    if !pok.verify(&commitment, &base, &challenge) {
        return Err(Error::BadRequest);
    }

    let e = epoch_fr(epoch);
    let sig = Signature23G1::new_with_committed_messages(
        rng,
        &commitment,
        BTreeMap::from([(IDX_EPOCH, &e)]),
        &key.0,
        &params.sig,
    )?;

    let mut out = vec![VERSION];
    write(&sig, &mut out);
    Ok(out)
}

/// A credential the holder can present. Store it encrypted at rest.
#[derive(Clone)]
pub struct Credential {
    link_secret: Fr,
    epoch: u64,
    sig: Signature23G1<Bls12_381>,
}

impl Drop for Credential {
    fn drop(&mut self) {
        self.link_secret.zeroize();
    }
}

impl Credential {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![VERSION];
        write(&self.link_secret, &mut out);
        out.extend_from_slice(&self.epoch.to_be_bytes());
        write(&self.sig, &mut out);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = read_versioned(bytes, "credential")?;
        let link_secret = read(&mut r, "credential")?;
        let epoch = read_u64(&mut r, "credential")?;
        let sig = read(&mut r, "credential")?;
        Ok(Self {
            link_secret,
            epoch,
            sig,
        })
    }
}

/// Holder: unwrap the issuer's signature into a credential, checking it
/// against the published issuer key.
pub fn finalize(
    params: &Params,
    pending: PendingCredential,
    signature: &[u8],
    epoch: u64,
    issuer: &IssuerPublicKey,
) -> Result<Credential, Error> {
    let mut r = read_versioned(signature, "signature")?;
    let sig: Signature23G1<Bls12_381> = read(&mut r, "signature")?;
    let messages = [pending.link_secret, epoch_fr(epoch)];
    sig.verify(&messages, issuer.prepared.clone(), params.prepared.clone())
        .map_err(|_| Error::BadSignature)?;
    Ok(Credential {
        link_secret: pending.link_secret,
        epoch,
        sig,
    })
}

// --- presentation -----------------------------------------------------------

fn present_challenge(params: &Params, context: &[u8], contribution: &[u8]) -> Fr {
    let mut bytes = PRESENT_DOMAIN.to_vec();
    bytes.extend_from_slice(params.label.as_bytes());
    bytes.extend_from_slice(&(context.len() as u64).to_be_bytes());
    bytes.extend_from_slice(context);
    bytes.extend_from_slice(contribution);
    compute_random_oracle_challenge::<Fr, Blake2b512>(&bytes)
}

/// Holder: prove possession of a credential, revealing only its epoch.
pub fn present<R: RngCore + CryptoRng>(
    params: &Params,
    credential: &Credential,
    context: &[u8],
    rng: &mut R,
) -> Result<Vec<u8>, Error> {
    let e = epoch_fr(credential.epoch);
    let protocol = PoKOfSignature23G1Protocol::init(
        rng,
        None,
        None,
        &credential.sig,
        &params.sig,
        [
            MessageOrBlinding::BlindMessageRandomly(&credential.link_secret),
            MessageOrBlinding::RevealMessage(&e),
        ],
    )?;
    let revealed = BTreeMap::from([(IDX_EPOCH, e)]);
    let mut contribution = Vec::new();
    protocol.challenge_contribution(&revealed, &params.sig, &mut contribution)?;
    let challenge = present_challenge(params, context, &contribution);
    let proof = protocol.gen_proof(&challenge)?;

    let mut out = vec![VERSION];
    out.extend_from_slice(&credential.epoch.to_be_bytes());
    write(&proof, &mut out);
    Ok(out)
}

/// Relay: verify a presentation for `context`. `accept_epoch` decides which
/// epochs are currently valid. Returns the presented epoch.
pub fn verify(
    params: &Params,
    issuer: &IssuerPublicKey,
    presentation: &[u8],
    context: &[u8],
    accept_epoch: impl Fn(u64) -> bool,
) -> Result<u64, Error> {
    let mut r = read_versioned(presentation, "presentation")?;
    let epoch = read_u64(&mut r, "presentation")?;
    let proof: PoKOfSignature23G1Proof<Bls12_381> = read(&mut r, "presentation")?;
    if !r.is_empty() {
        return Err(Error::Malformed("presentation"));
    }
    if !accept_epoch(epoch) {
        return Err(Error::Epoch(epoch));
    }

    let revealed = BTreeMap::from([(IDX_EPOCH, epoch_fr(epoch))]);
    let mut contribution = Vec::new();
    proof.challenge_contribution(&revealed, &params.sig, &mut contribution)?;
    let challenge = present_challenge(params, context, &contribution);
    proof
        .verify(
            &revealed,
            &challenge,
            issuer.prepared.clone(),
            params.prepared.clone(),
        )
        .map_err(|_| Error::BadPresentation)?;
    Ok(epoch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ark_std::rand::{SeedableRng, rngs::StdRng};

    struct Setup {
        params: Params,
        sk: IssuerSecretKey,
        pk: IssuerPublicKey,
        rng: StdRng,
    }

    fn setup() -> Setup {
        let mut rng = StdRng::seed_from_u64(7);
        let params = Params::new(DEFAULT_PARAMS_LABEL);
        let sk = IssuerSecretKey::generate(&mut rng);
        let pk = sk.public_key(&params);
        Setup { params, sk, pk, rng }
    }

    fn enroll(s: &mut Setup, epoch: u64) -> Credential {
        let (pending, req) = request(&s.params, b"session", &mut s.rng);
        let sig = issue(&s.params, &s.sk, &req, b"session", epoch, &mut s.rng).unwrap();
        finalize(&s.params, pending, &sig, epoch, &s.pk).unwrap()
    }

    #[test]
    fn issue_present_verify() {
        let mut s = setup();
        let cred = enroll(&mut s, 42);
        let p = present(&s.params, &cred, b"relay-a|nonce", &mut s.rng).unwrap();
        assert_eq!(verify(&s.params, &s.pk, &p, b"relay-a|nonce", |e| e == 42).unwrap(), 42);
    }

    #[test]
    fn presentation_is_bound_to_context() {
        let mut s = setup();
        let cred = enroll(&mut s, 42);
        let p = present(&s.params, &cred, b"relay-a|nonce", &mut s.rng).unwrap();
        assert!(matches!(
            verify(&s.params, &s.pk, &p, b"relay-b|nonce", |_| true),
            Err(Error::BadPresentation)
        ));
    }

    #[test]
    fn rejects_unaccepted_epoch() {
        let mut s = setup();
        let cred = enroll(&mut s, 41);
        let p = present(&s.params, &cred, b"ctx", &mut s.rng).unwrap();
        assert!(matches!(
            verify(&s.params, &s.pk, &p, b"ctx", |e| e == 42),
            Err(Error::Epoch(41))
        ));
    }

    #[test]
    fn rejects_other_issuer() {
        let mut s = setup();
        let cred = enroll(&mut s, 42);
        let p = present(&s.params, &cred, b"ctx", &mut s.rng).unwrap();
        let other = IssuerSecretKey::generate(&mut s.rng).public_key(&s.params);
        assert!(verify(&s.params, &other, &p, b"ctx", |_| true).is_err());
    }

    #[test]
    fn rejects_lying_about_epoch() {
        let mut s = setup();
        let cred = enroll(&mut s, 41);
        let mut p = present(&s.params, &cred, b"ctx", &mut s.rng).unwrap();
        p[1..9].copy_from_slice(&42u64.to_be_bytes());
        assert!(verify(&s.params, &s.pk, &p, b"ctx", |_| true).is_err());
    }

    #[test]
    fn issuer_rejects_request_for_other_session() {
        let mut s = setup();
        let (_, req) = request(&s.params, b"session-1", &mut s.rng);
        assert!(matches!(
            issue(&s.params, &s.sk, &req, b"session-2", 42, &mut s.rng),
            Err(Error::BadRequest)
        ));
    }

    #[test]
    fn presentations_are_randomized() {
        let mut s = setup();
        let cred = enroll(&mut s, 42);
        let a = present(&s.params, &cred, b"ctx", &mut s.rng).unwrap();
        let b = present(&s.params, &cred, b"ctx", &mut s.rng).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn serialization_round_trips() {
        let mut s = setup();
        let cred = enroll(&mut s, 42);
        let cred = Credential::from_bytes(&cred.to_bytes()).unwrap();
        let pk = IssuerPublicKey::from_bytes(&s.pk.to_bytes()).unwrap();
        let sk = IssuerSecretKey::from_bytes(&s.sk.to_bytes()).unwrap();
        assert_eq!(sk.public_key(&s.params).to_bytes(), pk.to_bytes());
        let p = present(&s.params, &cred, b"ctx", &mut s.rng).unwrap();
        verify(&s.params, &pk, &p, b"ctx", |_| true).unwrap();
    }
}
