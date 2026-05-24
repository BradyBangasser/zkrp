mod behavior;

use futures::StreamExt;
use libghost::{identity::NodeIdentity, transport::TransportConfig};
use libp2p::{SwarmBuilder, identify, mdns, noise, swarm::SwarmEvent, yamux};
use std::error::Error;
use tracing::{Level, debug, info};

use crate::behavior::{MeshBehavior, MeshBehaviorEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let identity = NodeIdentity::generate();
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "9000".to_string())
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

    info!("Listening for incoming mesh connections...");

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::ConnectionClosed{
                    connection_id,
                    peer_id,
                    cause,
                    ..
                } => {
                    swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
                    debug!("Peer {} disconnect (cid: {}), reason: {:?}", peer_id, connection_id, cause);
                },
                SwarmEvent::Behaviour(MeshBehaviorEvent::Mdns(mdns::Event::Discovered(peers))) => {
                    for (peer_id, multiaddr) in peers {
                        info!("mDNS Discovered: {} at {}", peer_id, multiaddr);
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr);
                    }
                },
                SwarmEvent::Behaviour(MeshBehaviorEvent::Mdns(mdns::Event::Expired(peers))) => {
                    for (peer_id, multiaddr) in peers {
                        info!("mDNS Expired: {} at {}", peer_id, multiaddr);
                        swarm.behaviour_mut().kademlia.remove_address(&peer_id, &multiaddr);
                    }
                },
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Listening on {address}");
                    info!("Relay addr: {address}/p2p/{}", swarm.local_peer_id());
                },
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
