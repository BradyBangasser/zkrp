//! gRPC surface for mailboxes.
//!
//!   Deposit / Fetch -- S3 store-and-forward. Knowing the mailbox id is the
//!                      capability to deposit to / fetch from it.
//!   Register        -- a device's sealed push token for a mailbox; a deposit
//!                      hands the mailbox's tokens to the push service.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};

use crate::config::PushConfig;
use crate::mailbox_store::{MailboxStore, StoreError};

pub mod proto {
    tonic::include_proto!("zrp.mailbox.v1");
}

mod push_proto {
    tonic::include_proto!("zrp.push.v1");
}

use proto::mailbox_service_server::MailboxService;
use proto::{
    DepositRequest, DepositResponse, Envelope, FetchRequest, FetchResponse, RegisterRequest,
};

/// One push per mailbox per this long: a burst of messages is one alert.
const PUSH_THROTTLE: Duration = Duration::from_secs(30);

pub struct MailboxServiceImpl {
    pub store: MailboxStore,
    pub push: Option<Pusher>,
}

/// Hands sealed tokens to the push service. Remembers when each mailbox was
/// last pushed, in memory, for the throttle window only.
#[derive(Clone)]
pub struct Pusher {
    url: String,
    recent: Arc<Mutex<HashMap<String, Instant>>>,
}

impl Pusher {
    pub fn new(config: &PushConfig) -> Self {
        Self {
            url: config.url.clone(),
            recent: Default::default(),
        }
    }

    /// False when this mailbox was pushed within the throttle window.
    fn due(&self, mailbox_id: &str) -> bool {
        let now = Instant::now();
        let mut recent = self.recent.lock().unwrap();
        recent.retain(|_, at| now.duration_since(*at) < PUSH_THROTTLE);
        recent.insert(mailbox_id.to_string(), now).is_none()
    }

    async fn notify(&self, sealed_tokens: Vec<Vec<u8>>) -> Result<(), Box<dyn std::error::Error>> {
        let channel = tonic::transport::Endpoint::from_shared(self.url.clone())?
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(10))
            .connect()
            .await?;
        push_proto::push_service_client::PushServiceClient::new(channel)
            .notify(push_proto::NotifyRequest { sealed_tokens })
            .await?;
        Ok(())
    }
}

fn record(result: &'static str) {
    telemetry::counter!("zkrp_push_requests_total", "result" => result).increment(1);
}

fn map_store_err(e: StoreError) -> Status {
    match e {
        StoreError::BadId => Status::invalid_argument("bad mailbox_id or tag"),
        StoreError::Empty => Status::invalid_argument("empty ciphertext"),
        StoreError::TooLarge => Status::resource_exhausted("ciphertext too large"),
        StoreError::S3(m) => Status::unavailable(format!("storage: {m}")),
    }
}

#[tonic::async_trait]
impl MailboxService for MailboxServiceImpl {
    async fn deposit(
        &self,
        req: Request<DepositRequest>,
    ) -> Result<Response<DepositResponse>, Status> {
        let r = req.into_inner();
        let msg_id = self
            .store
            .deposit(&r.mailbox_id, r.ciphertext)
            .await
            .map_err(map_store_err)?;

        // Wake the mailbox's other devices, after answering the sender.
        if let Some(pusher) = self.push.clone()
            && pusher.due(&r.mailbox_id)
        {
            let store = self.store.clone();
            tokio::spawn(async move {
                let tokens = match store.sealed_tokens(&r.mailbox_id, &r.sender_tag).await {
                    Ok(t) if t.is_empty() => return record("none"),
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!("push registrations: {e}");
                        return record("store_error");
                    }
                };
                match pusher.notify(tokens).await {
                    Ok(()) => record("ok"),
                    Err(e) => {
                        tracing::warn!("push service: {e}");
                        record("push_error");
                    }
                }
            });
        }

        Ok(Response::new(DepositResponse { msg_id }))
    }

    async fn fetch(&self, req: Request<FetchRequest>) -> Result<Response<FetchResponse>, Status> {
        let r = req.into_inner();
        let (envs, has_more) = self
            .store
            .fetch(&r.mailbox_id, &r.after, r.limit as i32)
            .await
            .map_err(map_store_err)?;

        Ok(Response::new(FetchResponse {
            envelopes: envs
                .into_iter()
                .map(|e| Envelope {
                    msg_id: e.msg_id,
                    ciphertext: e.ciphertext,
                    stored_at: e.stored_at_millis,
                })
                .collect(),
            has_more,
        }))
    }

    async fn register(&self, req: Request<RegisterRequest>) -> Result<Response<()>, Status> {
        if self.push.is_none() {
            return Err(Status::unimplemented("push isn't enabled on this relay"));
        }
        let r = req.into_inner();
        self.store
            .register(&r.mailbox_id, &r.tag, r.sealed_token)
            .await
            .map_err(map_store_err)?;
        Ok(Response::new(()))
    }
}
