use tonic::transport::Channel;

pub mod proto {
    tonic::include_proto!("zrp.relay.v1");
}

use proto::UploadChunk;
use proto::logging_client::LoggingClient;
use proto::relay_service_client::RelayServiceClient;
use proto::{HealthResponse, NetworkConfig, RelayInfo, RelayListResponse};

#[derive(Clone)]
pub struct RelayClient {
    inner: RelayServiceClient<Channel>,
}

impl RelayClient {
    pub async fn connect(addr: &str) -> Result<Self, tonic::transport::Error> {
        let inner = RelayServiceClient::new(crate::grpc::connect(addr).await?);
        Ok(Self { inner })
    }

    pub async fn list_relays(&mut self) -> Result<Vec<RelayInfo>, tonic::Status> {
        let response: tonic::Response<RelayListResponse> =
            self.inner.list_relays(tonic::Request::new(())).await?;
        Ok(response.into_inner().relays)
    }

    /// Auth URL, CDN domain, and epoch length, as configured on the relay.
    pub async fn network_config(&mut self) -> Result<NetworkConfig, tonic::Status> {
        Ok(self.inner.get_network_config(tonic::Request::new(())).await?.into_inner())
    }

    /// The relay's libp2p peer id, which gRPC admissions are bound to.
    pub async fn peer_id(&mut self) -> Result<String, tonic::Status> {
        Ok(self.inner.health(tonic::Request::new(())).await?.into_inner().peer_id)
    }

    pub async fn health(&mut self) -> Result<bool, tonic::Status> {
        let response: tonic::Response<HealthResponse> =
            self.inner.health(tonic::Request::new(())).await?;
        Ok(response.into_inner().status == 0)
    }

    pub async fn upload_debug_log(
        grpc_addr: &str,
        data: Vec<u8>,
        store: &dyn crate::store::GhostStore,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let channel = crate::grpc::connect(grpc_addr).await?;
        let mut client = LoggingClient::new(channel);

        let chunks: Vec<UploadChunk> = data
            .chunks(64 * 1024)
            .map(|c| UploadChunk { data: c.to_vec() })
            .collect();

        let mut request = tonic::Request::new(tokio_stream::iter(chunks));
        crate::admit::attach_grpc_admission(&mut request, store, grpc_addr, crate::admit::SERVICE_LOGGING)
            .await;
        let resp = client.upload_debug_log(request).await?.into_inner();
        Ok(resp.log_id)
    }
}
