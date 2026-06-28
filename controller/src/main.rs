mod behavior;
mod blob;
mod relay;
use crate::behavior::{MeshBehavior, MeshBehaviorEvent};
use crate::blob::BlobService;
use crate::proto::blob_store_server::BlobStoreServer;
use crate::relay::RelayServiceImpl;
use futures::StreamExt;
use libghost::{identity::NodeIdentity, transport::TransportConfig};
use libp2p::{SwarmBuilder, identify, mdns, noise, swarm::SwarmEvent, yamux};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tracing::{Level, debug, info};

pub mod proto {
    tonic::include_proto!("zrp.relay.v1");
}

use crate::proto::relay_service_server::RelayServiceServer;

#[derive(Clone)]
pub struct RelayState {
    pub peer_id: String,
    pub port: u32,
    pub connected_peers: Arc<Mutex<Vec<libp2p::PeerId>>>,
    pub messages_relayed: Arc<std::sync::atomic::AtomicU64>,
    pub started_at: std::time::Instant,
}

async fn serve(port: u16, state: RelayState) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port).parse()?;

    Server::builder()
        .add_service(RelayServiceServer::new(RelayServiceImpl { state }))
        .add_service(BlobStoreServer::new(
            BlobService::new("fratrat".into()).await,
        ))
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

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
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(MeshBehavior::new)?
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
    };

    let grpc_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = serve(grpc_port, grpc_state).await {
            tracing::error!("gRPC server error: {e}");
        }
    });

    info!("Listening for incoming mesh connections...");
    info!("gRPC on port {grpc_port}");

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    state.connected_peers.lock().await.push(peer_id);
                }
                SwarmEvent::ConnectionClosed { connection_id, peer_id, cause, .. } => {
                    swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
                    state.connected_peers.lock().await.retain(|p| p != &peer_id);
                    debug!("Peer {} disconnect (cid: {}), reason: {:?}", peer_id, connection_id, cause);
                }
                SwarmEvent::Behaviour(MeshBehaviorEvent::Mdns(mdns::Event::Discovered(peers))) => {
                    for (peer_id, multiaddr) in peers {
                        info!("mDNS Discovered: {} at {}", peer_id, multiaddr);
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);
                    }
                }
                SwarmEvent::Behaviour(MeshBehaviorEvent::Mdns(mdns::Event::Expired(peers))) => {
                    for (peer_id, multiaddr) in peers {
                        info!("mDNS Expired: {} at {}", peer_id, multiaddr);
                        swarm.behaviour_mut().kademlia.remove_address(&peer_id, &multiaddr);
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Listening on {address}");
                    info!("Relay addr: {address}/p2p/{}", swarm.local_peer_id());
                }
                SwarmEvent::Behaviour(MeshBehaviorEvent::Identify(
                    identify::Event::Sent { peer_id, .. }
                )) => {
                    info!("Sent identify to {peer_id}");
                }
                SwarmEvent::Behaviour(MeshBehaviorEvent::Identify(
                    identify::Event::Received { peer_id, info, connection_id }
                )) => {
                    info!("Identify received from {peer_id} CONN {}", connection_id);
                    for addr in &info.listen_addrs {
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                    }
                }
                other => info!("Swarm Event: {:?}", other),
            }
        }
    }
}
