mod api;
mod behavior;
mod config;

use crate::behavior::{RelayBehavior, RelayBehaviorEvent};
use crate::config::RelayConfig;
use futures::StreamExt;
use libghost::{identity::NodeIdentity, transport::TransportConfig};
use libp2p::{Multiaddr, SwarmBuilder, identify, noise, swarm::SwarmEvent, yamux};
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

/// The addresses this relay is reachable at from the public internet.
///
/// Prefers `RELAY_PUBLIC_ADDRS` (comma-separated multiaddrs, no `/p2p/...`
/// suffix), e.g.
///   `/ip4/116.203.218.60/tcp/9000,/dns4/relay.a.central.eu.infra.zkrp.net/tcp/9000`
/// Falls back to a public-IP lookup so a fresh deploy isn't dead on arrival.
async fn external_addrs(port: u16) -> Vec<Multiaddr> {
    if let Ok(raw) = std::env::var("RELAY_PUBLIC_ADDRS") {
        let addrs: Vec<Multiaddr> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .filter_map(|s| match s.parse::<Multiaddr>() {
                Ok(ma) => Some(ma),
                Err(e) => {
                    tracing::error!("RELAY_PUBLIC_ADDRS: bad multiaddr {s:?}: {e}");
                    None
                }
            })
            .collect();
        if !addrs.is_empty() {
            return addrs;
        }
    }

    match public_ip::addr_v4().await {
        Some(ip) => {
            tracing::warn!("RELAY_PUBLIC_ADDRS unset; discovered public IP {ip}");
            ["tcp", "quic"]
                .iter()
                .filter_map(|proto| {
                    let s = match *proto {
                        "tcp" => format!("/ip4/{ip}/tcp/{port}"),
                        _ => format!("/ip4/{ip}/udp/{port}/quic-v1"),
                    };
                    s.parse().ok()
                })
                .collect()
        }
        None => Vec::new(),
    }
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

    // NOTE: no `.with_relay_client(...)` in the chain now — the relay is never
    // itself relayed, so `with_behaviour` takes a single-arg closure.
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

    // ROOT CAUSE FIX.
    //
    // `relay::Behaviour` starts in `Status::Disable` and only flips to `Enable`
    // once it has a CONFIRMED external address (see
    // `determine_relay_status_from_external_address` in libp2p-relay). While
    // disabled it does not advertise `/libp2p/circuit/relay/0.2.0/hop`, so
    // every client's `listen_on(.../p2p-circuit)` fails protocol negotiation
    // and no reservation can ever be granted.
    //
    // Nothing confirms an address for us: `ExternalAddrConfirmed` would have to
    // come from autonat, and autonat needs reachable probe peers we don't have.
    // Our public address is static, so assert it.
    let external = external_addrs(port).await;
    if external.is_empty() {
        tracing::error!(
            "No external address — relay will stay in Status::Disable and \
             will NOT accept circuit reservations. Set RELAY_PUBLIC_ADDRS."
        );
    }
    for addr in external {
        info!("Confirming external address {addr}");
        swarm.add_external_address(addr);
    }

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
                        // NOTE: deliberately NOT calling kademlia.remove_peer here.
                        // Mobile peers reconnect constantly; evicting them on every
                        // disconnect kept the routing table permanently empty, which
                        // is why the relay logged "Failed to trigger bootstrap: No
                        // known peers" every 5 minutes even with peers connected.
                        state.connected_peers.lock().await.retain(|p| p != &peer_id);
                    }
                    debug!("Peer {peer_id} conn closed, remaining={num_established}, cause={cause:?}");
                }

                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Listening on {address}");
                    info!("Relay addr: {address}/p2p/{}", swarm.local_peer_id());
                }

                SwarmEvent::Behaviour(RelayBehaviorEvent::Identify(
                    identify::Event::Received { peer_id, info, connection_id }
                )) => {
                    info!("Identify received from {peer_id} conn={connection_id}");
                    for addr in &info.listen_addrs {
                        if !libghost::addr::is_dialable(addr) {
                            continue;
                        }
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                    }
                }

                SwarmEvent::Behaviour(RelayBehaviorEvent::Identify(
                    identify::Event::Sent { peer_id, .. }
                )) => {
                    debug!("Sent identify to {peer_id}");
                }

                // Reservation accept/deny was previously invisible: these fell
                // into the `other => debug!` arm below, under a Level::INFO
                // subscriber. Every reservation outcome now shows up in journald.
                SwarmEvent::Behaviour(RelayBehaviorEvent::RelayServer(e)) => {
                    info!("relay server: {e:?}");
                }

                SwarmEvent::ExternalAddrConfirmed { address } => {
                    info!("External address confirmed: {address} (hop advertisement enabled)");
                }

                other => debug!("{other:?}"),
            }
        }
    }
}
