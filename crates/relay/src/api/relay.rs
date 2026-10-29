use crate::RelayState;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::IntervalStream;
use tonic::{Request, Response, Status};

#[cfg(not(debug_assertions))]
async fn get_fallback_address(_: &Request<()>) -> String {
    public_ip::addr_v4()
        .await
        .and_then(|ip| Some(ip.to_string()))
        .unwrap_or("127.0.0.1".into())
}

#[cfg(debug_assertions)]
async fn get_fallback_address(req: &Request<()>) -> String {
    req.local_addr().unwrap().ip().to_string()
}

use crate::api::proto::{
    DeregisterRequest, HealthResponse, HealthStatus, NetworkConfig, RegisterRequest, RelayInfo, RelayListResponse, RelayStats,
    StatsResponse, WatchStatsRequest, relay_service_server::RelayService,
};

type WatchStatsStream = Pin<Box<dyn futures::Stream<Item = Result<StatsResponse, Status>> + Send>>;

pub struct RelayServiceImpl {
    pub state: RelayState,
}

#[tonic::async_trait]
impl RelayService for RelayServiceImpl {
    async fn health(&self, _: Request<()>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: health_status(&self.state) as i32,
            peer_id: self.state.peer_id.clone(),
            uptime_seconds: self.state.started_at.elapsed().as_secs(),
            version: telemetry::version(),
            meshes: self.state.config.meshes.clone(),
        }))
    }

    async fn stats(&self, _: Request<()>) -> Result<Response<StatsResponse>, Status> {
        Ok(Response::new(build_stats(&self.state)))
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
        .map(move |_| Ok(build_stats(&state)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_relays(&self, req: Request<()>) -> Result<Response<RelayListResponse>, Status> {
        let multiaddr = match &self.state.advertised_addr {
            Some(addr) => addr.clone(),
            None => self.guess_multiaddr(&req).await,
        };

        // This relay first (the client evidently reaches it), then the other
        // live relays, least loaded first.
        let mut relays = vec![RelayInfo {
            peer_id: self.state.peer_id.clone(),
            meshes: self.state.config.meshes.clone(),
            region: self.state.config.region.clone(),
            load: self.state.connected_peers.load(Ordering::Relaxed),
            multiaddr,
            port: self.state.port,
            grpc_url: self.state.config.public_grpc_url.clone(),
            ..Default::default()
        }];
        relays.extend(self.state.registry.peers().await);

        Ok(Response::new(RelayListResponse {
            relays,
            refresh_after_secs: 60,
        }))
    }

    async fn get_network_config(&self, _: Request<()>) -> Result<Response<NetworkConfig>, Status> {
        let c = &self.state.config;
        Ok(Response::new(NetworkConfig {
            auth_url: c.auth_url.clone(),
            blob_cdn_domain: c.cf_domain.clone(),
            epoch_secs: c.admission.as_ref().map(|a| a.epoch_secs).unwrap_or(0),
            meshes: c.meshes.clone(),
            refresh_after_secs: 3600,
            push_public_key: c.push.as_ref().map(|p| p.public_key.clone()).unwrap_or_default(),
        }))
    }

    // Relays join the registry by writing to it directly. An unauthenticated
    // Register RPC would let anyone add a relay to every client's list.
    async fn register(&self, _: Request<RegisterRequest>) -> Result<Response<()>, Status> {
        Err(Status::permission_denied("relays register through the shared registry"))
    }

    async fn deregister(&self, _: Request<DeregisterRequest>) -> Result<Response<()>, Status> {
        Err(Status::permission_denied("relays register through the shared registry"))
    }
}

impl RelayServiceImpl {
    /// Without RELAY_PUBLIC_ADDRS: PUBLIC_IP, else the request's host, else a
    /// public-IP lookup. gRPC over HTTP/2 rarely exposes the host, so set
    /// RELAY_PUBLIC_ADDRS in production.
    async fn guess_multiaddr(&self, req: &Request<()>) -> String {
        let (host_str, is_domain) = if let Ok(public) = std::env::var("PUBLIC_IP") {
            let is_domain = public.parse::<IpAddr>().is_err();
            (public, is_domain)
        } else if let Some(authority) = req
            .metadata()
            .get("host")
            .or_else(|| req.metadata().get(":authority"))
        {
            let raw = authority.to_str().unwrap_or("").to_string();
            let host = raw.split(':').next().unwrap_or(&raw).to_string();
            let is_domain = host.parse::<IpAddr>().is_err();
            (host, is_domain)
        } else {
            tracing::warn!("No host header on request, falling back to loopback");
            let host_str = get_fallback_address(req).await;
            let is_domain = host_str.parse::<IpAddr>().is_err();
            (host_str, is_domain)
        };

        let prefix = if is_domain {
            "dns4"
        } else {
            match host_str.parse::<IpAddr>() {
                Ok(IpAddr::V4(_)) => "ip4",
                Ok(IpAddr::V6(_)) => "ip6",
                Err(_) => "dns4",
            }
        };

        format!(
            "/{}/{}/tcp/{}/p2p/{}",
            prefix, host_str, self.state.port, self.state.peer_id
        )
    }
}

/// DEGRADED still serves traffic: without an external address the relay can't
/// grant reservations, and without the registry discovery is self-only.
/// Neither is fixed by a restart.
pub(crate) fn health_status(state: &RelayState) -> HealthStatus {
    if state.has_external_addr && state.registry.is_healthy() {
        HealthStatus::Healthy
    } else {
        HealthStatus::Degraded
    }
}

fn build_stats(state: &RelayState) -> StatsResponse {
    StatsResponse {
        current: Some(RelayStats {
            connected_peers: state.connected_peers.load(Ordering::Relaxed),
            active_reservations: state.active_reservations.load(Ordering::Relaxed),
            uptime_seconds: state.started_at.elapsed().as_secs(),
            ..Default::default()
        }),
        ..Default::default()
    }
}
