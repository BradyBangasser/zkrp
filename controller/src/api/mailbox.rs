//! gRPC surface for mailboxes.
//!
//!   Deposit / Fetch      -- S3 store-and-forward (active). Knowing the mailbox
//!                           id is the capability to deposit to / fetch from it.
//!   Register / Unregister -- push-token registry (dormant until push ships).

use tonic::{Request, Response, Status};

use libghost::mailbox::{MailboxStore, StoreError};

pub mod proto {
    tonic::include_proto!("zrp.mailbox.v1");
}

use proto::mailbox_service_server::MailboxService;
use proto::{DepositRequest, DepositResponse, Envelope, FetchRequest, FetchResponse};

pub struct MailboxServiceImpl {
    pub store: MailboxStore,
}

fn map_store_err(e: StoreError) -> Status {
    match e {
        StoreError::BadId => Status::invalid_argument("bad mailbox_id"),
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
}
