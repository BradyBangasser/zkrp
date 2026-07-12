use crate::mailer::Mailer;
use crate::proto::auth_service_server::AuthService;
use crate::proto::{ChallengeRequest, ChallengeResponse, RedeemRequest, RedeemResponse};
use base64::Engine;
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// The client hashes exactly these bytes to form the App Attest clientDataHash.
#[derive(serde::Deserialize)]
struct ClientData {
    challenge: String, // base64url, no pad
    age_gate_passed: bool,
}

pub struct Auth {
    pub mailer: Arc<dyn Mailer>,
    /// Lowercase, no leading dot: ["umn.edu", "iastate.edu"]
    pub allowed_domains: Vec<String>,
}

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

fn unb64(s: &str) -> Result<Vec<u8>, Status> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .map_err(|_| Status::invalid_argument("bad base64"))
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

    fn verify_email(&self, email: &str) -> bool {
        true
    }
}

#[tonic::async_trait]
impl AuthService for Auth {
    async fn get_challenge(
        &self,
        req: Request<ChallengeRequest>,
    ) -> Result<Response<ChallengeResponse>, Status> {
        let req = req.into_inner();

        if !self.verify_email(&req.email) {
            return Err(Status::invalid_argument("Invalid Email Address"));
        }

        self.mailer
            .send_link(&req.email, "test")
            .await
            .map_err(|e| Status::from_error(e.into()))?;

        Ok(Response::new(ChallengeResponse {
            challenge: "".into(),
        }))
    }

    async fn redeem(
        &self,
        req: Request<RedeemRequest>,
    ) -> Result<Response<RedeemResponse>, Status> {
        let req = req.into_inner();

        Err(Status::unimplemented("Challenges not yet implemented"))
    }
}
