mod api;
mod behavior;
mod config;

use crate::behavior::{RelayBehavior, RelayBehaviorEvent};
use crate::config::RelayConfig;
use futures::StreamExt;
use libghost::{identity::NodeIdentity, transport::TransportConfig};
use libp2p::{SwarmBuilder, identify, noise, swarm::SwarmEvent, yamux};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{Level, debug, info};

#[derive(Clone)]
pub struct RelayState {
    pub peer_id: String,
    pub port: u32,
    pub connected_peers: Arc<Mutex<Vec<libp2p::PeerId>>>,
    pub messages_relayed: Arc<std::sync::atomic::AtomicU64>,
    pub started_at: std::time::Instant,
    pub config: RelayConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = dotenv::dotenv();
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let relay_config = config::load_config().await;

    let identity = NodeIdentity::generate();
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "9000".to_string())
        .parse::<u16>()?;
    let grpc_port = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "9001".to_string())
        .parse::<u16>()?;
    let config = TransportConfig::with_ports(port, port);

    info!("Relay PeerID: {}", identity.peer_id_string());

    let mut swarm = SwarmBuilder::with_existing_identity(identity.keypair)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_behaviour(RelayBehavior::new)?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(config.idle_connection_timeout))
        .build();

    swarm.listen_on(config.quic_listen_addr().parse()?)?;
    swarm.listen_on(config.tcp_listen_addr().parse()?)?;

    let state = RelayState {
        peer_id: swarm.local_peer_id().to_string(),
        connected_peers: Arc::new(Mutex::new(Vec::new())),
        messages_relayed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        started_at: std::time::Instant::now(),
        port: port.into(),
        config: relay_config,
    };

    let grpc_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = crate::api::serve(grpc_port, grpc_state).await {
            tracing::error!("gRPC server error: {e}");
        }
    });

    info!("Listening for incoming mesh connections...");
    info!("gRPC on port {grpc_port}");

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    let mut peers = state.connected_peers.lock().await;
                    if !peers.contains(&peer_id) {
                        peers.push(peer_id);
                    }
                    info!("Peer connected: {peer_id}");
                }
                SwarmEvent::ConnectionClosed { peer_id, num_established, cause, .. } => {
                    if num_established == 0 {
                        state.connected_peers.lock().await.retain(|p| p != &peer_id);
                    }
                    debug!("Peer {peer_id} disconnected, remaining: {num_established}, cause: {cause:?}");
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Listening on {address}");
                    info!("Relay addr: {address}/p2p/{}", swarm.local_peer_id());
                }
                SwarmEvent::Behaviour(RelayBehaviorEvent::Identify(
                    identify::Event::Sent { peer_id, .. }
                )) => {
                    info!("Sent identify to {peer_id}");
                }
                SwarmEvent::Behaviour(RelayBehaviorEvent::Identify(
                    identify::Event::Received { peer_id, connection_id, .. }
                )) => {
                    info!("Identify received from {peer_id} conn={connection_id}");
                }
                other => debug!("{other:?}"),
            }
        }
    }
}
