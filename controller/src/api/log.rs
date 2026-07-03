use crate::{
    api::proto::{LogUploadResponse, UploadChunk, logging_server::Logging},
    config::RelayConfig,
};
use aws_sdk_s3::{self as s3, primitives::ByteStream};
use chrono::prelude::*;
use hex::ToHex;
use ring::rand::SecureRandom;
use tonic::{Response, Status};

const MAX_LOG_BYTES: usize = 10 * 1024 * 1024;

pub struct LogService {
    client: s3::Client,
    config: RelayConfig,
}

impl LogService {
    pub async fn new(conf: RelayConfig) -> Self {
        let config = aws_config::load_from_env().await;

        let client = s3::Client::new(&config);
        let now = Utc::now();

        client
            .put_object()
            .bucket(&conf.log_bucket)
            .key(format!(
                "logs/start/{}/{}/{}",
                now.year(),
                now.month(),
                now.day0()
            ))
            .body(ByteStream::from_static("I started!".as_bytes()))
            .content_type("text/plain")
            .send()
            .await
            .expect("Failed to write log");
        Self {
            client,
            config: conf,
        }
    }
}

#[tonic::async_trait]
impl Logging for LogService {
    async fn upload_debug_log(
        &self,
        request: tonic::Request<tonic::Streaming<UploadChunk>>,
    ) -> Result<Response<LogUploadResponse>, Status> {
        let mut stream = request.into_inner();
        let mut data: Vec<u8> = Vec::new();

        while let Some(chunk) = stream.message().await.unwrap() {
            data.extend_from_slice(&chunk.data);
            if data.len() > MAX_LOG_BYTES {
                return Err(Status::resource_exhausted("blob exceeds 10 MB limit"));
            }
        }

        if data.is_empty() {
            return Err(Status::invalid_argument("empty upload"));
        }

        let mut id_buffer = [0u8; 32];
        let sr = ring::rand::SystemRandom::new();
        sr.fill(&mut id_buffer).unwrap();
        let blob_id = id_buffer.encode_hex::<String>();
        let blob = format!("log/{}/{}", &blob_id[0..3], &blob_id[3..]);
        tracing::info!("Inserting blob {}", blob);
        self.client
            .put_object()
            .bucket(&self.config.blob_bucket)
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
