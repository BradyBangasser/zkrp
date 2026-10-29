// async_trait (ours and tonic's generated server traits) adds #[must_use] to
// futures that already are; clippy 1.99+ flags the expansion, not our code.
#![allow(clippy::double_must_use)]

mod admission;
mod api;
mod behavior;
mod config;
mod mailbox_store;
mod registry;

use crate::admission::Admission;
use crate::behavior::{RelayBehavior, RelayBehaviorEvent};
use crate::config::RelayConfig;
use crate::registry::{Registry, SelfEntry};
use futures::StreamExt;
use libghost::admit::AdmitResponse;
use libghost::{identity::NodeIdentity, transport::TransportConfig};
use libp2p::request_response::{self, ResponseChannel};
use libp2p::{
    Multiaddr, PeerId, Swarm, SwarmBuilder, identify, noise, relay, swarm::SwarmEvent, yamux,
};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tracing::{debug, info, warn};

pub type BoxError = Box<dyn Error + Send + Sync>;

/// Shared between the swarm loop and the gRPC services. Deliberately holds
/// counts, not peer ids: the relay has no reason to remember who is connected.
#[derive(Clone)]
pub struct RelayState {
    pub peer_id: String,
    pub port: u32,
    pub connected_peers: Arc<AtomicU32>,
    pub active_reservations: Arc<AtomicU32>,
    pub active_circuits: Arc<AtomicU32>,
    pub started_at: std::time::Instant,
    pub config: RelayConfig,
    pub registry: Registry,
    /// False when no public address could be determined; the relay then
    /// cannot grant reservations and reports DEGRADED.
    pub has_external_addr: bool,
    /// None when ADMISSION=off.
    pub admission: Option<Arc<Admission>>,
    /// `<public tcp addr>/p2p/<peer id>`: what ListRelays and the registry
    /// advertise. None without RELAY_PUBLIC_ADDRS or a discovered public IP.
    pub advertised_addr: Option<String>,
}

/// How long a new peer has to present a credential before it is dropped.
const ADMIT_DEADLINE: Duration = Duration::from_secs(10);

/// How long a disconnected peer stays in the routing table. Clients get a
/// new transport id every launch, so one that stays away isn't coming back
/// under this id; keeping it only makes every newcomer dial a dead peer and
/// spend the relay's circuit budget. A brief drop (a phone changing
/// networks) reconnects well within this.
const DEPARTED_GRACE: Duration = Duration::from_secs(120);

/// Peer bookkeeping for the swarm loop. Peer ids live here only while their
/// connection does (or, in `departed`, for the grace period after).
#[derive(Default)]
struct Gate {
    pending: HashMap<PeerId, std::time::Instant>,
    admitted: HashSet<PeerId>,
    departed: HashMap<PeerId, std::time::Instant>,
}

type Verdict = (PeerId, ResponseChannel<AdmitResponse>, bool);

/// The addresses this relay is reachable at from the public internet.
///
/// Prefers `RELAY_PUBLIC_ADDRS` (comma-separated multiaddrs, no `/p2p/...`
/// suffix), e.g.
///   `/ip4/203.0.113.7/tcp/9000,/dns4/relay.example.net/tcp/9000`
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

    match tokio::time::timeout(Duration::from_secs(10), public_ip::addr_v4()).await {
        Ok(Some(ip)) => {
            warn!("RELAY_PUBLIC_ADDRS unset; discovered public IP {ip}");
            [
                format!("/ip4/{ip}/tcp/{port}"),
                format!("/ip4/{ip}/udp/{port}/quic-v1"),
            ]
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect()
        }
        _ => Vec::new(),
    }
}

/// Load the relay keypair, creating it on first boot. A corrupt key file is an
/// error rather than a silent rotation: a new peer id breaks every client that
/// cached this relay's address.
fn load_identity(path: &Path) -> Result<NodeIdentity, BoxError> {
    match std::fs::read(path) {
        Ok(bytes) => NodeIdentity::from_keypair_bytes(&bytes)
            .map_err(|e| format!("identity {}: {e}", path.display()).into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let identity = NodeIdentity::generate();
            if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::create_dir_all(dir)?;
            }
            let tmp = path.with_extension("tmp");
            {
                use std::io::Write;
                #[cfg(unix)]
                use std::os::unix::fs::OpenOptionsExt;
                let mut opts = std::fs::OpenOptions::new();
                opts.write(true).create(true).truncate(true);
                #[cfg(unix)]
                opts.mode(0o600);
                let mut f = opts.open(&tmp)?;
                f.write_all(&identity.to_keypair_bytes())?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp, path)?;
            info!("Generated new relay identity at {}", path.display());
            Ok(identity)
        }
        Err(e) => Err(format!("identity {}: {e}", path.display()).into()),
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

fn main() -> ExitCode {
    let _ = dotenv::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // `zkrp-relay healthcheck` is the container HEALTHCHECK; it needs no
    // credentials, so it runs before config is loaded.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return api::healthcheck();
    }

    // Reads credentials into the environment, so it must run before the
    // runtime spawns any threads.
    let config = config::load_config();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    // `zkrp-relay reports [list | show <id>]`: read user reports (api/log.rs).
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("reports") {
        return match runtime.block_on(api::reports_cli(config, &args[1..])) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        };
    }

    match runtime.block_on(run(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("relay exiting: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(relay_config: RelayConfig) -> Result<(), BoxError> {
    let identity = load_identity(&relay_config.identity_path)?;
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "9000".to_string())
        .parse::<u16>()?;
    let grpc_port = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "9001".to_string())
        .parse::<u16>()?;
    let config = TransportConfig::with_ports(port, port);

    telemetry::install("0.0.0.0:9464");

    info!("zkrp-relay {}", telemetry::version());
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

    // `relay::Behaviour` starts in `Status::Disable` and only advertises
    // `/libp2p/circuit/relay/0.2.0/hop` once it has a CONFIRMED external
    // address. Nothing confirms one for us (autonat needs reachable probe
    // peers), and our public address is static, so assert it.
    let external = external_addrs(port).await;
    if external.is_empty() {
        tracing::error!(
            "No external address — relay will stay in Status::Disable and \
             will NOT accept circuit reservations. Set RELAY_PUBLIC_ADDRS."
        );
    }
    for addr in &external {
        info!("Confirming external address {addr}");
        swarm.add_external_address(addr.clone());
    }

    let peer_id = swarm.local_peer_id().to_string();

    let admission = match &relay_config.admission {
        Some(a) => Some(Arc::new(
            Admission::new(
                &a.params_label,
                &a.issuer_public_key,
                a.epoch_secs,
                swarm.local_peer_id(),
            )
            .map_err(|e| format!("ISSUER_PUBLIC_KEY: {e}"))?,
        )),
        None => None,
    };

    let registry = match &relay_config.database_url {
        Some(url) => Registry::connect(url)?,
        None => Registry::standalone(),
    };

    // Advertise the first public TCP address; clients dial over TCP.
    let advertised_addr = external
        .iter()
        .find(|a| {
            a.iter()
                .any(|p| matches!(p, libp2p::multiaddr::Protocol::Tcp(_)))
        })
        .map(|a| format!("{a}/p2p/{peer_id}"));

    let state = RelayState {
        peer_id: peer_id.clone(),
        connected_peers: Arc::new(AtomicU32::new(0)),
        active_reservations: Arc::new(AtomicU32::new(0)),
        active_circuits: Arc::new(AtomicU32::new(0)),
        started_at: std::time::Instant::now(),
        port: port.into(),
        has_external_addr: !external.is_empty(),
        registry: registry.clone(),
        config: relay_config.clone(),
        admission: admission.clone(),
        advertised_addr: advertised_addr.clone(),
    };

    if let Some(multiaddr) = advertised_addr {
        tokio::spawn(registry.clone().run(
            SelfEntry {
                peer_id: peer_id.clone(),
                multiaddr,
                grpc_url: relay_config.public_grpc_url.clone(),
                region: relay_config.region.clone(),
                meshes: relay_config.meshes.clone(),
            },
            state.connected_peers.clone(),
        ));
    }

    tokio::spawn(export_gauges(state.clone()));
    tokio::spawn(api::serve_healthz(state.clone()));

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(());
    let mut grpc = tokio::spawn(api::serve(grpc_port, state.clone(), async move {
        let mut rx = stop_rx;
        let _ = rx.changed().await;
    }));

    info!("Listening for incoming mesh connections...");
    info!("gRPC on port {grpc_port}");

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let mut gate = Gate::default();
    let (verdict_tx, mut verdict_rx) = tokio::sync::mpsc::channel::<Verdict>(256);
    let mut sweep = tokio::time::interval(Duration::from_secs(2));

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown requested");
                break;
            }
            // The gRPC server only returns on failure. A relay without its
            // API is unreachable for discovery, so exit and let the
            // supervisor restart us rather than limp along half-alive.
            res = &mut grpc => {
                return Err(match res {
                    Ok(Ok(())) => "gRPC server stopped unexpectedly".into(),
                    Ok(Err(e)) => format!("gRPC server failed: {e}").into(),
                    Err(e) => format!("gRPC server panicked: {e}").into(),
                });
            }
            event = swarm.select_next_some() => {
                handle_event(&mut swarm, &state, &mut gate, &verdict_tx, event)
            }
            Some((peer, channel, ok)) = verdict_rx.recv() => {
                let response = if ok { AdmitResponse::Admitted } else { AdmitResponse::Denied };
                let _ = swarm.behaviour_mut().admit.send_response(channel, response);
                if ok {
                    gate.pending.remove(&peer);
                    gate.admitted.insert(peer);
                } else {
                    let _ = swarm.disconnect_peer_id(peer);
                }
            }
            _ = sweep.tick() => {
                let now = std::time::Instant::now();
                let gone: Vec<PeerId> = gate
                    .departed
                    .iter()
                    .filter(|(_, at)| now.duration_since(**at) >= DEPARTED_GRACE)
                    .map(|(p, _)| *p)
                    .collect();
                for peer in gone {
                    gate.departed.remove(&peer);
                    swarm.behaviour_mut().kademlia.remove_peer(&peer);
                }

                let expired: Vec<PeerId> = gate
                    .pending
                    .iter()
                    .filter(|(_, deadline)| **deadline <= now)
                    .map(|(p, _)| *p)
                    .collect();
                for peer in expired {
                    gate.pending.remove(&peer);
                    telemetry::counter!("zkrp_peers_dropped_unadmitted_total").increment(1);
                    debug!("Peer {peer} not admitted in time; disconnecting");
                    let _ = swarm.disconnect_peer_id(peer);
                }
            }
        }
    }

    let _ = stop_tx.send(());
    registry.deregister(&peer_id).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), grpc).await;
    info!("Relay stopped");
    Ok(())
}

/// Point-in-time gauges, refreshed every few seconds.
async fn export_gauges(state: RelayState) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        telemetry::gauge!("zkrp_connected_peers")
            .set(state.connected_peers.load(Ordering::Relaxed));
        telemetry::gauge!("zkrp_active_reservations")
            .set(state.active_reservations.load(Ordering::Relaxed));
        telemetry::gauge!("zkrp_active_circuits")
            .set(state.active_circuits.load(Ordering::Relaxed));
        telemetry::gauge!("zkrp_registry_healthy").set(state.registry.is_healthy() as u8);
        // 0 = HEALTHY, 1 = DEGRADED, 2 = UNHEALTHY (proto HealthStatus).
        telemetry::gauge!("zkrp_relay_health_status").set(api::relay::health_status(&state) as i32);
        telemetry::gauge!("zkrp_uptime_seconds").set(state.started_at.elapsed().as_secs_f64());
    }
}

fn dec(counter: &AtomicU32) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
}

/// Peer ids and addresses are logged at DEBUG only: at the default level the
/// relay's logs should not be a record of who connected when.
fn handle_event(
    swarm: &mut Swarm<RelayBehavior>,
    state: &RelayState,
    gate: &mut Gate,
    verdicts: &tokio::sync::mpsc::Sender<Verdict>,
    event: SwarmEvent<RelayBehaviorEvent>,
) {
    match event {
        SwarmEvent::ConnectionEstablished {
            peer_id,
            num_established,
            ..
        } => {
            if num_established.get() == 1 {
                gate.departed.remove(&peer_id);
                state.connected_peers.fetch_add(1, Ordering::Relaxed);
                if state.admission.is_some() && !gate.admitted.contains(&peer_id) {
                    gate.pending
                        .insert(peer_id, std::time::Instant::now() + ADMIT_DEADLINE);
                }
            }
            debug!("Peer connected: {peer_id}");
        }

        SwarmEvent::Behaviour(RelayBehaviorEvent::Admit(request_response::Event::Message {
            peer,
            message:
                request_response::Message::Request {
                    request, channel, ..
                },
            ..
        })) => {
            let Some(adm) = state.admission.clone() else {
                let _ = swarm
                    .behaviour_mut()
                    .admit
                    .send_response(channel, AdmitResponse::Admitted);
                return;
            };
            // The pairing check takes milliseconds; keep it off the swarm loop.
            let tx = verdicts.clone();
            tokio::task::spawn_blocking(move || {
                let me = adm.relay_peer_id().to_vec();
                let client = peer.to_bytes();
                let scope = zkcred::Scope::P2p {
                    relay_peer_id: &me,
                    client_peer_id: &client,
                };
                let result = adm.check(scope, &request);
                admission::record("p2p", &result);
                if let Err(e) = &result {
                    debug!("admission from {peer} refused: {e}");
                }
                let _ = tx.blocking_send((peer, channel, result.is_ok()));
            });
        }

        SwarmEvent::ConnectionClosed {
            peer_id,
            num_established,
            cause,
            ..
        } => {
            // Not evicted from the routing table right away: mobile peers
            // reconnect constantly, and evicting on every disconnect kept the
            // table empty. The sweep drops peers gone for DEPARTED_GRACE.
            if num_established == 0 {
                dec(&state.connected_peers);
                gate.pending.remove(&peer_id);
                gate.admitted.remove(&peer_id);
                gate.departed.insert(peer_id, std::time::Instant::now());
            }
            debug!("Peer {peer_id} conn closed, remaining={num_established}, cause={cause:?}");
        }

        SwarmEvent::NewListenAddr { address, .. } => {
            info!("Listening on {address}/p2p/{}", swarm.local_peer_id());
        }

        SwarmEvent::Behaviour(RelayBehaviorEvent::Identify(identify::Event::Received {
            peer_id,
            info,
            ..
        })) => {
            for addr in info.listen_addrs {
                if libghost::addr::is_dialable(&addr) {
                    swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                }
            }
        }

        SwarmEvent::Behaviour(RelayBehaviorEvent::RelayServer(e)) => {
            match &e {
                relay::Event::ReservationReqAccepted { renewed: false, .. } => {
                    state.active_reservations.fetch_add(1, Ordering::Relaxed);
                }
                relay::Event::ReservationClosed { .. }
                | relay::Event::ReservationTimedOut { .. } => {
                    dec(&state.active_reservations);
                }
                relay::Event::CircuitReqAccepted { .. } => {
                    state.active_circuits.fetch_add(1, Ordering::Relaxed);
                }
                relay::Event::CircuitClosed { .. } => dec(&state.active_circuits),
                relay::Event::ReservationReqDenied { .. }
                | relay::Event::CircuitReqDenied { .. } => {
                    info!("relay server denied a request (limits reached)");
                }
                _ => {}
            }
            debug!("relay server: {e:?}");
        }

        SwarmEvent::ExternalAddrConfirmed { address } => {
            info!("External address confirmed: {address} (hop advertisement enabled)");
        }

        other => debug!("{other:?}"),
    }
}
