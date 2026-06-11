use crate::RelayState;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::IntervalStream;
use tonic::{Request, Response, Status, transport::Server};

pub mod proto {
    tonic::include_proto!("zrp.relay.v1");
}

use proto::{
    DeregisterRequest, HealthResponse, RegisterRequest, RelayInfo, RelayListResponse, RelayStats,
    StatsResponse, WatchStatsRequest,
    relay_service_server::{RelayService, RelayServiceServer},
};

type WatchStatsStream = Pin<Box<dyn futures::Stream<Item = Result<StatsResponse, Status>> + Send>>;

pub struct RelayServiceImpl {
    state: RelayState,
}

#[tonic::async_trait]
impl RelayService for RelayServiceImpl {
    async fn health(&self, _: Request<()>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: 0,
            peer_id: self.state.peer_id.clone(),
            uptime_seconds: self.state.started_at.elapsed().as_secs(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            meshes: vec!["fratrat-v1".to_string()],
        }))
    }

    async fn stats(&self, _: Request<()>) -> Result<Response<StatsResponse>, Status> {
        Ok(Response::new(self.build_stats().await))
    }

    type WatchStatsStream = WatchStatsStream;

    async fn watch_stats(
        &self,
        request: Request<WatchStatsRequest>,
    ) -> Result<Response<Self::WatchStatsStream>, Status> {
        let interval_secs = request.into_inner().interval_secs.max(1);
        let state = self.state.clone();

        let stream = IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(
            interval_secs,
        )))
        .then(move |_| {
            let state = state.clone();
            async move {
                let connected = state.connected_peers.lock().await.len() as u32;
                let messages = state.messages_relayed.load(Ordering::Relaxed);
                Ok(StatsResponse {
                    current: Some(RelayStats {
                        connected_peers: connected,
                        messages_relayed: messages,
                        uptime_seconds: state.started_at.elapsed().as_secs(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_relays(&self, req: Request<()>) -> Result<Response<RelayListResponse>, Status> {
        let ip_str = if let Ok(public_ip) = std::env::var("PUBLIC_IP") {
            public_ip
        } else if let Some(remote_addr) = req.remote_addr() {
            remote_addr.ip().to_string()
        } else {
            return Err(Status::unknown(
                "Cannot determine public IP — set PUBLIC_IP env var",
            ));
        };

        let is_ipv6 = ip_str.contains(':');
        let prefix = if is_ipv6 { "ip6" } else { "ip4" };

        let multiaddr = format!(
            "/{}/{}/tcp/{}/p2p/{}",
            prefix, ip_str, self.state.port, self.state.peer_id
        );

        println!("{}", req.remote_addr().unwrap());

        Ok(Response::new(RelayListResponse {
            relays: vec![RelayInfo {
                peer_id: self.state.peer_id.clone(),
                meshes: vec!["fratrat-v1".to_string()],
                region: std::env::var("REGION").unwrap_or_else(|_| "local".to_string()),
                load: self.state.connected_peers.lock().await.len() as u32,
                multiaddr,
                port: self.state.port,
                ..Default::default()
            }],
            refresh_after_secs: 60,
        }))
    }

    async fn register(&self, request: Request<RegisterRequest>) -> Result<Response<()>, Status> {
        let info = request
            .into_inner()
            .info
            .ok_or_else(|| Status::invalid_argument("missing relay info"))?;
        tracing::info!("Relay registered: {} ({})", info.peer_id, info.region);
        // TODO: store in a shared registry for multi-relay setups
        Ok(Response::new(()))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<()>, Status> {
        let peer_id = request.into_inner().peer_id;
        tracing::info!("Relay deregistered: {}", peer_id);
        // TODO: remove from shared registry
        Ok(Response::new(()))
    }
}

impl RelayServiceImpl {
    async fn build_stats(&self) -> StatsResponse {
        let connected = self.state.connected_peers.lock().await.len() as u32;
        let messages = self.state.messages_relayed.load(Ordering::Relaxed);
        StatsResponse {
            current: Some(RelayStats {
                connected_peers: connected,
                messages_relayed: messages,
                uptime_seconds: self.state.started_at.elapsed().as_secs(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

pub async fn serve(port: u16, state: RelayState) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port).parse()?;

    Server::builder()
        .add_service(RelayServiceServer::new(RelayServiceImpl { state }))
        .serve(addr)
        .await?;

    Ok(())
}
