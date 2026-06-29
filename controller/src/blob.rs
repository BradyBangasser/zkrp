use aws_sdk_s3 as s3;
use ring::rand::SecureRandom;
use std::pin::Pin;

use hex::ToHex;

use futures::Stream;
use tonic::{Request, Response, Status};

use crate::proto::{
    BlobChunk, DownloadRequest, UploadChunk, UploadResponse, blob_store_server::BlobStore,
};

const MAX_BLOB_BYTES: usize = 10 * 1024 * 1024;

pub struct BlobService {
    client: s3::Client,
    bucket: String,
}

impl BlobService {
    pub async fn new(bucket: String) -> Self {
        let config = aws_config::load_from_env().await;

        Self {
            client: s3::Client::new(&config),
            bucket,
        }
    }
}

#[tonic::async_trait]
impl BlobStore for BlobService {
    type DownloadBlobStream =
        Pin<Box<dyn Stream<Item = Result<BlobChunk, Status>> + Send + 'static>>;

    async fn download_blob(
        &self,
        _request: Request<DownloadRequest>,
    ) -> Result<Response<Self::DownloadBlobStream>, Status> {
        Err(Status::unimplemented(
            "use CloudFront URL from upload response",
        ))
    }

    async fn upload_blob(
        &self,
        request: Request<tonic::Streaming<UploadChunk>>,
    ) -> Result<Response<UploadResponse>, Status> {
        let mut stream = request.into_inner();
        let mut data: Vec<u8> = Vec::new();

        while let Some(chunk) = stream.message().await.unwrap() {
            data.extend_from_slice(&chunk.data);
            if data.len() > MAX_BLOB_BYTES {
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
        let blob = format!("/blob/{}/{}", &blob_id[0..3], &blob_id[3..]);
        tracing::info!("Inserting blob {}", blob);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&blob)
            .body(data.into())
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Failed to upload blob: {:?}", e.to_string());
                Status::internal(format!("s3 put: {}", e))
            })?;

        Ok(Response::new(UploadResponse { blob_id: blob }))
    }
}
