use crate::{
    api::proto::{LogUploadResponse, UploadChunk, logging_server::Logging},
    config::RelayConfig,
};
use aws_sdk_s3 as s3;
use hex::ToHex;
use ring::rand::SecureRandom;
use tonic::{Response, Status};

const MAX_LOG_BYTES: usize = 10 * 1024 * 1024;

/// gRPC metadata marking an upload as a user report rather than a debug log
/// (`libghost::relay::RelayClient::upload_report`). Reports are filed under
/// `report/` so they can be read and kept apart from logs.
pub const UPLOAD_KIND_KEY: &str = "x-zkrp-upload-kind";
const REPORT_PREFIX: &str = "report";

pub struct LogService {
    client: s3::Client,
    config: RelayConfig,
}

impl LogService {
    pub fn new(client: s3::Client, config: RelayConfig) -> Self {
        Self { client, config }
    }
}

#[tonic::async_trait]
impl Logging for LogService {
    async fn upload_debug_log(
        &self,
        request: tonic::Request<tonic::Streaming<UploadChunk>>,
    ) -> Result<Response<LogUploadResponse>, Status> {
        let prefix = match request.metadata().get(UPLOAD_KIND_KEY).map(|v| v.as_bytes()) {
            Some(b"report") => REPORT_PREFIX,
            _ => "log",
        };
        let mut stream = request.into_inner();
        let mut data: Vec<u8> = Vec::new();

        loop {
            match stream.message().await {
                Ok(Some(chunk)) => {
                    data.extend_from_slice(&chunk.data);
                    if data.len() > MAX_LOG_BYTES {
                        return Err(Status::resource_exhausted("blob exceeds 10 MB limit"));
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!("log upload stream aborted: {e}");
                    return Err(Status::aborted("upload stream interrupted"));
                }
            }
        }

        if data.is_empty() {
            return Err(Status::invalid_argument("empty upload"));
        }

        let mut id_buffer = [0u8; 32];
        let sr = ring::rand::SystemRandom::new();
        sr.fill(&mut id_buffer)
            .map_err(|_| Status::internal("rng unavailable"))?;
        let blob_id = id_buffer.encode_hex::<String>();
        let blob = format!("{prefix}/{}/{}", &blob_id[0..3], &blob_id[3..]);
        tracing::debug!("Inserting log {}", blob);
        self.client
            .put_object()
            .bucket(&self.config.log_bucket)
            .key(&blob)
            .body(data.into())
            .content_type("text/plain")
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Failed to upload blob: {:?}", e.to_string());
                Status::internal(format!("s3 put: {}", e))
            })?;

        Ok(Response::new(LogUploadResponse { log_id: blob }))
    }
}

/// `zkrp-relay reports [list | show <id>]`: read the reports filed on the
/// log bucket, using the relay's own S3 settings. For operators, on a relay
/// host: `docker compose exec relay zkrp-relay reports list`.
pub async fn reports_cli(
    config: RelayConfig,
    args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let client = s3::Client::new(&aws_config::load_from_env().await);
    match args.first().map(String::as_str) {
        None | Some("list") => {
            let mut pages = client
                .list_objects_v2()
                .bucket(&config.log_bucket)
                .prefix(format!("{REPORT_PREFIX}/"))
                .into_paginator()
                .send();
            let mut found = Vec::new();
            while let Some(page) = pages.next().await {
                for o in page?.contents() {
                    let when = o.last_modified().map(|t| t.to_string()).unwrap_or_default();
                    found.push((when, o.key().unwrap_or_default().to_string()));
                }
            }
            found.sort();
            for (when, key) in &found {
                println!("{when}  {key}");
            }
            eprintln!("{} report(s); `zkrp-relay reports show <id>` to read one", found.len());
        }
        Some("show") => {
            let id = args.get(1).ok_or("usage: zkrp-relay reports show <id>")?;
            if !id.starts_with(&format!("{REPORT_PREFIX}/")) {
                return Err("not a report id (they start with report/)".into());
            }
            let obj = client.get_object().bucket(&config.log_bucket).key(id).send().await?;
            let body = obj.body.collect().await?.into_bytes();
            println!("{}", String::from_utf8_lossy(&body));
        }
        Some(other) => return Err(format!("unknown reports command {other}").into()),
    }
    Ok(())
}
