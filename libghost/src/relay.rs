use tonic::transport::Channel;

pub mod proto {
    tonic::include_proto!("zrp.relay.v1");
}

use proto::UploadChunk;
use proto::logging_client::LoggingClient;
use proto::relay_service_client::RelayServiceClient;
use proto::{HealthResponse, RelayInfo, RelayListResponse};

#[derive(Clone)]
pub struct RelayClient {
    inner: RelayServiceClient<Channel>,
}

impl RelayClient {
    pub async fn connect(addr: &str) -> Result<Self, tonic::transport::Error> {
        let inner = RelayServiceClient::connect(addr.to_string()).await?;
        Ok(Self { inner })
    }

    pub async fn list_relays(&mut self) -> Result<Vec<RelayInfo>, tonic::Status> {
        let response: tonic::Response<RelayListResponse> =
            self.inner.list_relays(tonic::Request::new(())).await?;
        Ok(response.into_inner().relays)
    }

    pub async fn health(&mut self) -> Result<bool, tonic::Status> {
        let response: tonic::Response<HealthResponse> =
            self.inner.health(tonic::Request::new(())).await?;
        Ok(response.into_inner().status == 0)
    }

    pub async fn upload_debug_log(
        grpc_addr: &str,
        data: Vec<u8>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let channel = tonic::transport::Channel::from_shared(grpc_addr.to_string())?
            .connect()
            .await?;
        let mut client = LoggingClient::new(channel);

        let chunks: Vec<UploadChunk> = data
            .chunks(64 * 1024)
            .map(|c| UploadChunk { data: c.to_vec() })
            .collect();

        let resp = client
            .upload_debug_log(tokio_stream::iter(chunks))
            .await?
            .into_inner();
        Ok(resp.log_id)
    }
}
