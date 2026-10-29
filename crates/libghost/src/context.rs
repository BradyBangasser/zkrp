use crate::blob::{BlobManager, BlobManifest, ChunkRequest, ChunkResponse, ChunkStoreRequest};
use crate::relay::RelayClient;
use crate::{
    codec::{self, Codec},
    handler::{ConnectionStatus, DisconnectReason, EventHandler, SendFailReason, ZRPEvent},
    protocols::ghost::v0::{GhostEnvelope, GhostMessage, encode},
    sealed::Seals,
    store::GhostStore,
};
use libp2p::dns::{ResolverConfig, ResolverOpts};
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::{Multiaddr, PeerId, multiaddr};
use libp2p_core;
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use ulid::Ulid;
use zeroize::Zeroize;

const MAX_ATTEMPTS: u32 = 8;

fn backoff_delay(attempt: u32) -> Option<Duration> {
    if attempt >= MAX_ATTEMPTS {
        return None;
    }
    let secs = (2u64.pow(attempt - 1)).min(60);
    Some(Duration::from_secs(secs))
}

struct PendingMessage {
    id: String,
    topic: String,
    payload: Vec<u8>,
    attempts: u32,
    next_retry: tokio::time::Instant,
    content_type: u16,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OutboxEntry {
    id: String,
    topic: String,
    payload: Vec<u8>,
    attempts: u32,
    content_type: u16,
}

impl From<&PendingMessage> for OutboxEntry {
    fn from(m: &PendingMessage) -> Self {
        Self {
            id: m.id.clone(),
            topic: m.topic.clone(),
            payload: m.payload.clone(),
            attempts: m.attempts,
            content_type: m.content_type,
        }
    }
}

async fn swarm_task<B>(
    mut swarm: libp2p::Swarm<B>,
    relay_addrs: Vec<String>,
    transport_config: crate::transport::TransportConfig,
    mut cmd_rx: mpsc::Receiver<SwarmCommand>,
    raw_tx: mpsc::Sender<RawEvent>,
    store: Arc<dyn GhostStore>,
) where
    B: crate::traits::MeshBehaviour + Send + 'static,
    B::ToSwarm: Send,
{
    use futures::StreamExt;
    use libp2p::swarm::SwarmEvent;

    let mut circuit_reserved: std::collections::HashSet<libp2p::PeerId> =
        std::collections::HashSet::new();

    // Our own peer id — needed to build our advertised circuit address.
    let local_peer_id = *swarm.local_peer_id();

    let mut discover_interval = tokio::time::interval(Duration::from_secs(15));
    discover_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut has_reservation = false;

    let mut relay_multiaddrs: HashMap<PeerId, Multiaddr> = HashMap::new();
    // Relay peers we're connected to but haven't yet successfully reserved a
    // circuit on. Retried on the discovery tick — important on CGNAT/cellular
    // where the first listen_on(circuit) can fail on a not-yet-ready link.
    let mut pending_circuit: std::collections::HashSet<libp2p::PeerId> =
        std::collections::HashSet::new();

    // `listen_on(circuit)` returns Ok immediately — the reservation itself
    // succeeds or fails ASYNCHRONOUSLY, surfacing as ListenerClosed/
    // ListenerError, never as a relay::client::Event. Without this map we can't
    // tell which relay a dead listener belonged to, so a failed reservation
    // leaves the peer wedged in `circuit_reserved` forever with nothing
    // retrying it.
    let mut circuit_listeners: HashMap<libp2p_core::transport::ListenerId, PeerId> = HashMap::new();

    // --- Roaming / silent auto-heal state ---------------------------------
    // The set of peer ids that are RELAYS (parsed from relay_addrs). Lets us
    // distinguish "lost my relay" from "a mesh peer left" on ConnectionClosed.
    let relay_peer_ids: std::collections::HashSet<PeerId> = relay_addrs
        .iter()
        .filter_map(|r| r.parse::<libp2p::Multiaddr>().ok())
        .filter_map(|ma| {
            ma.iter().find_map(|p| match p {
                libp2p::multiaddr::Protocol::P2p(id) => Some(id),
                _ => None,
            })
        })
        .collect();
    // Relays we've lost and need to re-dial (network roam, drop, etc). Drained
    // by the reconnect tick with backoff.
    let mut relays_to_redial: std::collections::HashSet<PeerId> = std::collections::HashSet::new();
    let mut reconnect_backoff = tokio::time::interval(Duration::from_secs(3));
    reconnect_backoff.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Roam handling: only re-dial on network change once we've successfully
    // reserved at least once (avoids racing the initial startup dial), and
    // debounce the burst of NewListenAddr events an interface change produces.
    let mut ever_reserved = false;
    let mut last_roam_redial = tokio::time::Instant::now() - Duration::from_secs(10);


    let mut retry_interval = tokio::time::interval(Duration::from_millis(500));
    let mut outbox: Vec<PendingMessage> = store
        .list("ghost/outbox/")
        .into_iter()
        .filter_map(|key| store.get(&key))
        .filter_map(|bytes| postcard::from_bytes::<OutboxEntry>(&bytes).ok())
        .map(|e| PendingMessage {
            id: e.id,
            topic: e.topic,
            payload: e.payload,
            attempts: e.attempts,
            next_retry: tokio::time::Instant::now(),
            content_type: e.content_type,
        })
        .collect();

    if !outbox.is_empty() {
        tracing::info!("Loaded {} pending messages from outbox", outbox.len());
    }

    swarm
        .listen_on(transport_config.tcp_listen_addr().parse().unwrap())
        .unwrap();
    swarm
        .listen_on(transport_config.quic_listen_addr().parse().unwrap())
        .unwrap();

    for relay in &relay_addrs {
        match relay.parse::<libp2p::Multiaddr>() {
            Ok(addr) => {
                let _ = swarm.dial(addr);
            }
            Err(e) => {
                tracing::warn!("skipping unparseable relay addr {relay:?}: {e}");
            }
        }
    }

    loop {
        tokio::select! {
            biased;
            _ = retry_interval.tick() => {
            let now = tokio::time::Instant::now();
            let mut still_pending = Vec::new();

            for mut msg in outbox.drain(..) {
                if now < msg.next_retry {
                    still_pending.push(msg);
                    continue;
                }

                let topic = libp2p::gossipsub::IdentTopic::new(&msg.topic);
                match swarm.behaviour_mut().publish(topic, msg.payload.clone()) {
                    Ok(_) => {
                        store.delete(&format!("ghost/outbox/{}", msg.id));
                        tracing::info!("publish succeeded after {} attempts", msg.attempts + 1);
                    }

                    Err(libp2p::gossipsub::PublishError::NoPeersSubscribedToTopic) => {
                        msg.attempts += 1;
                        if let Some(delay) = backoff_delay(msg.attempts) {
                            msg.next_retry = now + delay;
                            tracing::debug!(
                                "publish retry {} in {:?}", msg.attempts, delay
                            );
                            store.set(
                                &format!("ghost/outbox/{}", msg.id),
                                postcard::to_allocvec(&OutboxEntry::from(&msg)).unwrap(),
                            );
                            still_pending.push(msg);
                        } else {
                            store.delete(&format!("ghost/outbox/{}", msg.id));
                            tracing::warn!("publish giving up after {} attempts", msg.attempts);
                            let _ = raw_tx.send(RawEvent::PublishFailed {
                                topic: msg.topic.clone(),
                            }).await;
                        }
                    }
                    Err(e) => {
                        store.delete(&format!("ghost/outbox/{}", msg.id));
                        tracing::warn!("publish failed (non-retryable): {e}");
                        let _ = raw_tx.send(RawEvent::PublishFailed {
                            topic: msg.topic.clone(),
                        }).await;
                    }
                }
            }

            outbox = still_pending;
        }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::Behaviour(b_event) => {
                        if let Some(mesh_event) = B::translate_event(&b_event) {
                            match mesh_event {
                                crate::traits::MeshEvent::PeerDiscovered { peer_id, addr } => {
                                    let _ = raw_tx.send(RawEvent::PeerDiscovered {
                                        peer_id, addr
                                    }).await;
                                }
                                crate::traits::MeshEvent::RelayReservationAccepted { peer_id } => {
                                    tracing::info!("Relay reservation accepted by {}", peer_id);
                                    has_reservation = true;
                                    ever_reserved = true;

                                    // Ensure we're listening on the circuit for this relay.
                                    // The initial listen_on happens in ConnectionEstablished,
                                    // but on high-latency/CGNAT links that can fail; re-affirm
                                    // it here now that the relay has confirmed the reservation.
                                    if let Some(relay_ma) = relay_multiaddrs.get(&peer_id) {
                                        let circuit = relay_ma.clone().with(multiaddr::Protocol::P2pCircuit);
                                        if !circuit_reserved.contains(&peer_id) {
                                            match swarm.listen_on(circuit.clone()) {
                                                Ok(id) => {
                                                    circuit_listeners.insert(id, peer_id);
                                                    circuit_reserved.insert(peer_id);
                                                    tracing::info!("Now listening on circuit {}", circuit);
                                                }
                                                Err(e) => tracing::warn!("Circuit listen (on accept) failed: {}", e),
                                            }
                                        }

                                        // CRITICAL: listen_on(circuit) only makes us ACCEPT inbound
                                        // circuit connections — it does NOT make identify advertise
                                        // the circuit address to other peers. We must explicitly
                                        // register the full circuit address (with our own peer id)
                                        // as an external address, or peers only ever learn our
                                        // direct (NAT'd, unreachable) addresses and can never dial
                                        // us through the relay. This is what makes two NAT'd/cellular
                                        // devices reachable to each other.
                                        let my_circuit = circuit
                                            .clone()
                                            .with(multiaddr::Protocol::P2p(local_peer_id));
                                        swarm.add_external_address(my_circuit.clone());
                                        tracing::info!("Advertising circuit external addr {}", my_circuit);
                                    }

                                    // Immediately kick a discovery walk so we find existing
                                    // peers without waiting for the next 15s tick.
                                    let key = libp2p::PeerId::random();
                                    swarm.behaviour_mut().kademlia_get_closest(key);

                                    let _ = raw_tx.send(RawEvent::RelayAccepted { relay_addr: peer_id }).await;
                                }
                                crate::traits::MeshEvent::RelayReservationFailed => {
                                    let _ = raw_tx.send(RawEvent::RelayLost).await;
                                }

                                crate::traits::MeshEvent::Identify { peer_id, listen_addrs } => {
                                    swarm.behaviour_mut().on_identify_received(peer_id, listen_addrs.clone());

                                    for addr in listen_addrs {
                                        if addr.to_string().contains("p2p-circuit")
                                        && !swarm.is_connected(&peer_id)
                                        {
                                            let _ = swarm.dial(
                                                DialOpts::peer_id(peer_id)
                                                    .addresses(vec![addr])
                                                    .condition(PeerCondition::Disconnected)
                                                    .build()
                                            );
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }

                        if let Some((topic, bytes, _forwarder, author)) = B::extract_gossip(&b_event) {
                            let _ = raw_tx.send(RawEvent::GossipMessage { topic, bytes, author, held: false }).await;
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                        circuit_reserved.remove(&peer_id);
                        pending_circuit.remove(&peer_id);
                        tracing::info!("Connection closed: {} cause: {:?}", peer_id, cause);

                        if relay_peer_ids.contains(&peer_id) {
                            // We lost a RELAY (roam, network change, drop). Queue
                            // it for silent re-dial and drop our reservation flag
                            // if we have no other relay left.
                            tracing::warn!("Lost relay {}, will re-dial", peer_id);
                            relays_to_redial.insert(peer_id);
                            relay_multiaddrs.remove(&peer_id);
                            // If that was our only relay, we no longer hold a
                            // reservation; discovery pauses until we re-reserve.
                            let still_on_a_relay = circuit_reserved
                                .iter()
                                .any(|p| relay_peer_ids.contains(p));
                            if !still_on_a_relay {
                                has_reservation = false;
                                let _ = raw_tx.send(RawEvent::RelayLost).await;
                            }
                        } else {
                            // A regular mesh peer left.
                            let _ = raw_tx.send(RawEvent::PeerLost { peer_id }).await;
                        }
                    }

                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        tracing::warn!("Outgoing connection error (peer={:?}): {}", peer_id, error);
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        tracing::warn!("Incoming connection error: {}", error);
                    }

                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        let addr = match &endpoint {
                            libp2p::core::ConnectedPoint::Dialer { address, .. } => address.clone(),
                            libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
                        };

                        // If this was a relay we'd lost, stop trying to re-dial it.
                        relays_to_redial.remove(&peer_id);

                        // Prove to the relay we hold a credential. Relays drop
                        // connections that aren't admitted within a few seconds.
                        if relay_peer_ids.contains(&peer_id) {
                            match crate::admit::p2p_admission(store.as_ref(), &peer_id, &local_peer_id) {
                                Some(admission) => swarm.behaviour_mut().send_admission(peer_id, admission),
                                None => tracing::warn!("no credential; relay {peer_id} will drop us. Enroll first."),
                            }
                        }

                        let _ = raw_tx.send(RawEvent::PeerDiscovered { peer_id, addr }).await;

                        if !circuit_reserved.contains(&peer_id) {
                            let circuit_addr = relay_addrs.iter().find_map(|r| {
                                let ma = r.parse::<libp2p::Multiaddr>().ok()?;
                                let has_peer = ma.iter().any(|p| {
                                    matches!(p, libp2p::multiaddr::Protocol::P2p(id) if id == peer_id)
                                });
                                if has_peer { Some(ma) } else { None }
                            });

                            if let Some(ma) = circuit_addr {
                                relay_multiaddrs.insert(peer_id, ma.clone());
                                circuit_reserved.insert(peer_id);
                                let circuit = ma.with(multiaddr::Protocol::P2pCircuit);
                                tracing::info!("Requesting circuit reservation via {}", circuit);
                                match swarm.listen_on(circuit) {
                                    Ok(id) => {
                                        circuit_listeners.insert(id, peer_id);
                                    }
                                    Err(e) => {
                                        tracing::warn!("Circuit reservation request failed: {} (will retry)", e);
                                        circuit_reserved.remove(&peer_id);
                                        // Remember it so the discovery tick retries — critical on
                                        // cellular where the first attempt often loses the race.
                                        pending_circuit.insert(peer_id);
                                    }
                                }
                            }
                        }
                    }

                    SwarmEvent::NewListenAddr { address, .. } => {
                        // A new local listen address appeared — usually means a
                        // network interface came up (roamed onto new WiFi/cellular).
                        // Proactively re-dial a lost relay rather than waiting for
                        // the dead connection to time out.
                        //
                        // Guards:
                        //  - At startup libp2p emits one NewListenAddr per
                        //    interface/protocol (loopback+LAN × TCP+QUIC = 4), while
                        //    the initial relay dial is already in flight. Re-dialing
                        //    then just collides (Address already in use). So we only
                        //    treat this as a roam AFTER we've had a reservation at
                        //    least once (ever_reserved), and we debounce bursts.
                        tracing::debug!("New listen addr {}", address);
                        let now = tokio::time::Instant::now();
                        let debounced = now.duration_since(last_roam_redial) < Duration::from_secs(2);
                        if ever_reserved && !debounced {
                            let connected_to_relay = relay_peer_ids
                                .iter()
                                .any(|p| swarm.is_connected(p));
                            if !connected_to_relay {
                                last_roam_redial = now;
                                tracing::info!("Network changed, no relay connected — re-dialing");
                                for r in &relay_addrs {
                                    if let Ok(ma) = r.parse::<libp2p::Multiaddr>() {
                                        let _ = swarm.dial(ma);
                                    }
                                }
                            }
                        }
                    }

                    // A circuit listener dying IS a failed/expired reservation.
                    // libp2p reports it here, not as a relay::client::Event —
                    // relay::client::Event has no failure variant at all. Until
                    // these arms existed the reason was silently discarded and
                    // the relay stayed stuck in `circuit_reserved`, so nothing
                    // ever retried it.
                    SwarmEvent::ListenerClosed { listener_id, reason, .. } => {
                        if let Some(relay_peer) = circuit_listeners.remove(&listener_id) {
                            tracing::warn!(
                                "Circuit listener for relay {} closed — reservation failed: {:?}",
                                relay_peer, reason
                            );
                            circuit_reserved.remove(&relay_peer);
                            if swarm.is_connected(&relay_peer) {
                                pending_circuit.insert(relay_peer);
                            }
                            if circuit_reserved.is_empty() {
                                has_reservation = false;
                                let _ = raw_tx.send(RawEvent::RelayLost).await;
                            }
                        } else {
                            tracing::debug!("Listener {:?} closed: {:?}", listener_id, reason);
                        }
                    }

                    SwarmEvent::ListenerError { listener_id, error } => {
                        if let Some(relay_peer) = circuit_listeners.get(&listener_id) {
                            tracing::warn!(
                                "Circuit listener error for relay {}: {}",
                                relay_peer, error
                            );
                        } else {
                            tracing::debug!("Listener {:?} error: {}", listener_id, error);
                        }
                    }

                    // Reservation was granted and has now lapsed (relay default
                    // is a 1h reservation_duration). Re-arm rather than sit dark.
                    SwarmEvent::ExpiredListenAddr { listener_id, address } => {
                        if let Some(relay_peer) = circuit_listeners.remove(&listener_id) {
                            tracing::warn!("Circuit address expired ({}), re-requesting", address);
                            circuit_reserved.remove(&relay_peer);
                            if swarm.is_connected(&relay_peer) {
                                pending_circuit.insert(relay_peer);
                            }
                        }
                    }

                    _ => {}
                }
            }

                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        SwarmCommand::Publish { topic, payload, content_type} => {
                            let msg = PendingMessage {
                                id: Ulid::new().to_string(),
                                topic,
                                payload,
                                attempts: 0,
                                next_retry: tokio::time::Instant::now(),
                                content_type
                            };
                            store.set(
                                &format!("ghost/outbox/{}", msg.id),
                                postcard::to_allocvec(&OutboxEntry::from(&msg)).unwrap(),
                            );
                            outbox.push(msg);
                        }
                        SwarmCommand::Subscribe { topic } => {
                            let topic = libp2p::gossipsub::IdentTopic::new(&topic);
                            if let Err(e) = swarm.behaviour_mut().subscribe_topic(topic) {
                                tracing::warn!("subscribe failed: {e}");
                            }
                        }
                        SwarmCommand::Unsubscribe { topic } => {
                            swarm
                                .behaviour_mut()
                                .unsubscribe_topic(libp2p::gossipsub::IdentTopic::new(&topic));
                        }
                        SwarmCommand::Shutdown => break,
                        // Handled by the crypto task; never forwarded here.
                        SwarmCommand::PublishHeld { .. } | SwarmCommand::InjectHeld { .. } => {}
                    }
                }
                _ = reconnect_backoff.tick() => {
                    // Silent auto-heal: re-dial any relay we've lost. On success
                    // ConnectionEstablished re-runs the reservation flow, which
                    // re-establishes the circuit and resumes discovery — the app
                    // never sees more than a brief gap (messages queue in outbox).
                    if !relays_to_redial.is_empty() {
                        let targets: Vec<PeerId> = relays_to_redial.iter().copied().collect();
                        for relay_peer in targets {
                            // If we've reconnected in the meantime, clear it.
                            if swarm.is_connected(&relay_peer) {
                                relays_to_redial.remove(&relay_peer);
                                continue;
                            }
                            // Find this relay's full multiaddr from the configured list.
                            if let Some(ma) = relay_addrs.iter().find_map(|r| {
                                let ma = r.parse::<libp2p::Multiaddr>().ok()?;
                                let matches = ma.iter().any(|p| {
                                    matches!(p, libp2p::multiaddr::Protocol::P2p(id) if id == relay_peer)
                                });
                                if matches { Some(ma) } else { None }
                            }) {
                                tracing::info!("Auto-heal: re-dialing relay {}", ma);
                                let _ = swarm.dial(ma);
                            } else {
                                relays_to_redial.remove(&relay_peer);
                            }
                        }
                    }
                }
                _ = discover_interval.tick() => {
                    // Retry any relay circuit reservations that failed initially.
                    // On cellular/CGNAT the first listen_on often loses a race with
                    // connection setup; this recovers those devices.
                    let retry: Vec<PeerId> = pending_circuit.iter().copied().collect();
                    for relay_peer in retry {
                        if let Some(relay_ma) = relay_multiaddrs.get(&relay_peer) {
                            let circuit = relay_ma.clone().with(multiaddr::Protocol::P2pCircuit);
                            match swarm.listen_on(circuit.clone()) {
                                Ok(id) => {
                                    tracing::info!("Circuit reservation retry issued: {}", circuit);
                                    circuit_listeners.insert(id, relay_peer);
                                    circuit_reserved.insert(relay_peer);
                                    pending_circuit.remove(&relay_peer);
                                }
                                Err(e) => tracing::warn!("Circuit retry still failing: {}", e),
                            }
                        } else {
                            pending_circuit.remove(&relay_peer);
                        }
                    }

                    if has_reservation {
                        let random_key = libp2p::PeerId::random();
                        swarm.behaviour_mut().kademlia_get_closest(random_key);
                    }
                }
            }
    }
}

/// Sign `payload` for `topic`, seal it if the topic is sealed, and encode
/// the envelope as it goes on the wire. None when sealed-only mode forbids
/// sending it in the clear.
fn outgoing(
    account: &libp2p::identity::Keypair,
    seals: &Seals,
    topic: &str,
    payload: &[u8],
    content_type: u16,
) -> Option<Vec<u8>> {
    tracing::info!("crypto_task: publishing topic={topic} content_type=0x{content_type:04x}");
    let mut message = GhostMessage::new(content_type, payload).ok()?;
    crate::signed::sign(account, topic, &mut message, crate::admit::now_unix());
    let signed = postcard::to_allocvec(&message).ok()?;
    let payload = match seals.key(topic) {
        Some(key) => {
            let sealed = crate::sealed::seal(&key, topic, &signed);
            postcard::to_allocvec(&GhostMessage::new(codec::CONTENT_TYPE_SEALED, &sealed).ok()?).ok()?
        }
        None if seals.required() => {
            tracing::warn!("sealed-only: not publishing on an unsealed topic");
            return None;
        }
        None => signed,
    };
    let mut encoded = Vec::new();
    encode(&payload).encode(&mut encoded).ok()?;
    Some(encoded)
}

#[allow(unused, clippy::too_many_arguments)]
async fn crypto_task(
    mut raw_rx: mpsc::Receiver<RawEvent>,
    mut crypto_rx: mpsc::Receiver<SwarmCommand>,
    swarm_tx: mpsc::Sender<SwarmCommand>,
    crypto_tx: mpsc::Sender<SwarmCommand>,
    handlers: Arc<Mutex<HashMap<String, Arc<dyn EventHandler>>>>,
    codecs: Arc<Mutex<HashMap<u16, Codec>>>,
    store: Arc<dyn GhostStore>,
    blob_manager: Arc<BlobManager>,
    account: libp2p::identity::Keypair,
    seals: Arc<Seals>,
    raw_tx: mpsc::Sender<RawEvent>,
) {
    // Signatures of recent messages: a message fetched from a mailbox may
    // also have arrived live, and handlers should see it once.
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut seen_order: std::collections::VecDeque<Vec<u8>> = std::collections::VecDeque::new();
    const SEEN_MAX: usize = 4096;

    // Transport peer -> the account it has proven (by signing) it speaks for,
    // and how many live transports each account has. Apps only ever see
    // account ids in PeerConnected / PeerDisconnected / Message.
    let mut accounts: HashMap<PeerId, PeerId> = HashMap::new();
    let mut present: HashMap<PeerId, usize> = HashMap::new();
    let mut transport_addrs: HashMap<PeerId, Multiaddr> = HashMap::new();

    loop {
        tokio::select! {
        Some(event) = raw_rx.recv() => {
            match event {
                RawEvent::GossipMessage { bytes, author, topic, held } => {

                    // Decode envelope
                    let envelope = match GhostEnvelope::decode(bytes.as_ref()) {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!("Failed to decode envelope: {}", e);
                            continue;
                        }
                    };

                    // Decode message
                    let msg: GhostMessage = match postcard::from_bytes(&envelope.payload) {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::warn!("Failed to deserialize message: {}", e);
                            continue;
                        }
                    };

                    // A sealed topic takes only sealed messages, and in
                    // sealed-only mode nothing is accepted in the clear.
                    let key = seals.key(&topic);
                    let msg = match (msg.codec == codec::CONTENT_TYPE_SEALED, key) {
                        (true, Some(key)) => {
                            let Some(inner) = crate::sealed::open(&key, &topic, &msg.payload)
                                .and_then(|b| postcard::from_bytes::<GhostMessage>(&b).ok())
                                .filter(|m| m.codec != codec::CONTENT_TYPE_SEALED)
                            else {
                                tracing::debug!("dropping unopenable sealed message on {topic}");
                                continue;
                            };
                            inner
                        }
                        (true, None) => continue,
                        (false, Some(_)) => continue,
                        (false, None) if seals.required() => continue,
                        (false, None) => msg,
                    };

                    // Attribute to the signing account, never to the forwarder.
                    let verify = if held { crate::signed::verify_held } else { crate::signed::verify };
                    let sender = match verify(&topic, &msg, crate::admit::now_unix()) {
                        Ok(account) => account,
                        Err(e) => {
                            tracing::debug!("dropping message on {topic}: {e}");
                            continue;
                        }
                    };

                    if !seen.insert(msg.signature.clone()) {
                        continue;
                    }
                    seen_order.push_back(msg.signature.clone());
                    if seen_order.len() > SEEN_MAX
                        && let Some(old) = seen_order.pop_front()
                    {
                        seen.remove(&old);
                    }

                    if let Some(transport) = author
                        && accounts.insert(transport, sender).is_none()
                    {
                        let n = present.entry(sender).or_insert(0);
                        *n += 1;
                        if *n == 1 {
                            blob_manager.on_peer_connected(sender.to_string()).await;
                            let addr = transport_addrs.get(&transport).cloned().unwrap_or_else(Multiaddr::empty);
                            let e = Arc::new(ZRPEvent::PeerConnected { peer_id: sender, addr });
                            for handler in handlers.lock().await.values() {
                                let h = handler.clone();
                                let e = e.clone();
                                let tx = crypto_tx.clone();
                                tokio::spawn(async move { h.handle(&e, tx) });
                            }
                        }
                    }

                    let peer_id_str = sender.to_string();

                    match msg.codec {
                        codec::CONTENT_TYPE_HELLO => continue,


                        codec::CONTENT_TYPE_CHUNK_STORE => {
                            if let Ok(req) = postcard::from_bytes::<ChunkStoreRequest>(&msg.payload) {
                                blob_manager.handle_store_request(req, &peer_id_str).await;
                            }
                            continue;
                        }

                        codec::CONTENT_TYPE_CHUNK_REQUEST => {
                            if let Ok(req) = postcard::from_bytes::<ChunkRequest>(&msg.payload) {
                                blob_manager.handle_chunk_request(req, &peer_id_str).await;
                            }
                            continue;
                        }

                        codec::CONTENT_TYPE_CHUNK_RESPONSE => {
                            if let Ok(resp) = postcard::from_bytes::<ChunkResponse>(&msg.payload) {
                                blob_manager.handle_chunk_response(resp, &peer_id_str).await;
                            }
                            continue;
                        }

                        codec::CONTENT_TYPE_BLOB_MANIFEST => {
                            if let Ok(manifest) = postcard::from_bytes::<BlobManifest>(&msg.payload) {
                                blob_manager.handle_manifest(manifest).await;
                            }
                            continue;
                        }

                        _ => {}
                    }

                    if msg.codec >= codec::RESERVED_CONTENT_TYPES {
                        continue;
                    }

                    let e = ZRPEvent::Message {
                        conversation: topic,
                        peer_id: sender,
                        content_type: msg.codec,
                        payload: msg.payload.clone(),
                    };

                    let event = Arc::new(e);
                    for handler in handlers.lock().await.values() {
                        let h = handler.clone();
                        let e = event.clone();
                        let tx = crypto_tx.clone();
                        tokio::spawn(async move { h.handle(&e, tx) });
                    }
                }

                RawEvent::PeerDiscovered { peer_id, addr } => {
                    // A new transport connection says nothing about who is on
                    // the other end. Announce our account; theirs arrives the
                    // same way, and PeerConnected fires once it verifies. The
                    // second hello covers a subscription exchange that hadn't
                    // finished by the first.
                    transport_addrs.insert(peer_id, addr);
                    let tx = crypto_tx.clone();
                    let seals = Arc::clone(&seals);
                    tokio::spawn(async move {
                        for delay in [2, 6] {
                            tokio::time::sleep(Duration::from_secs(delay)).await;
                            for topic in seals.hello_topics() {
                                let _ = tx
                                    .send(SwarmCommand::Publish {
                                        topic,
                                        payload: Vec::new(),
                                        content_type: codec::CONTENT_TYPE_HELLO,
                                    })
                                    .await;
                            }
                        }
                    });
                }

                RawEvent::PeerLost { peer_id } => {
                    transport_addrs.remove(&peer_id);
                    let Some(account) = accounts.remove(&peer_id) else {
                        continue;
                    };
                    let remaining = present.get_mut(&account).map(|n| {
                        *n = n.saturating_sub(1);
                        *n
                    });
                    if remaining != Some(0) {
                        continue;
                    }
                    present.remove(&account);
                    blob_manager.on_peer_disconnected(&account.to_string()).await;

                    let e = Arc::new(ZRPEvent::PeerDisconnected {
                        peer_id: account,
                        reason: DisconnectReason::Clean,
                    });
                    for handler in handlers.lock().await.values() {
                        let h = handler.clone();
                        let e = e.clone();
                        let tx = crypto_tx.clone();
                        tokio::spawn(async move { h.handle(&e, tx) });
                    }
                }

                RawEvent::RelayAccepted { relay_addr } => {
                    let e = Arc::new(ZRPEvent::ConnectionStatus(
                        ConnectionStatus::Connected { relay: relay_addr }
                    ));
                    for handler in handlers.lock().await.values() {
                        let h = handler.clone();
                        let e = e.clone();
                        let tx = crypto_tx.clone();
                        tokio::spawn(async move { h.handle(&e, tx) });
                    }
                }

                RawEvent::RelayLost => {
                    let e = Arc::new(ZRPEvent::ConnectionStatus(
                        ConnectionStatus::Disconnected
                    ));
                    for handler in handlers.lock().await.values() {
                        let h = handler.clone();
                        let e = e.clone();
                        let tx = crypto_tx.clone();
                        tokio::spawn(async move { h.handle(&e, tx) });
                    }
                }

                RawEvent::PublishFailed { topic } => {
                    let e = Arc::new(ZRPEvent::MessageSendFailed {
                        conversation: topic,
                        reason: SendFailReason::NoPeersSubscribed,
                    });
                    for handler in handlers.lock().await.values() {
                        let h = handler.clone();
                        let e = e.clone();
                        let tx = crypto_tx.clone();
                        tokio::spawn(async move { h.handle(&e, tx) });
                    }
                }
            }
        }

            Some(cmd) = crypto_rx.recv() => {
                match cmd {
                    SwarmCommand::Publish { topic, payload, content_type } => {
                        if let Some(encoded) = outgoing(&account, &seals, &topic, &payload, content_type) {
                            let _ = swarm_tx.send(SwarmCommand::Publish {
                                topic,
                                payload: encoded,
                                content_type
                            }).await;
                        }
                    }
                    SwarmCommand::PublishHeld { topic, payload, content_type, reply } => {
                        let encoded = outgoing(&account, &seals, &topic, &payload, content_type);
                        if let Some(encoded) = &encoded {
                            let _ = swarm_tx.send(SwarmCommand::Publish {
                                topic,
                                payload: encoded.clone(),
                                content_type
                            }).await;
                        }
                        let _ = reply.send(encoded);
                    }
                    SwarmCommand::InjectHeld { topic, bytes } => {
                        // Back through the receive path above, without
                        // blocking this loop on its own channel.
                        let tx = raw_tx.clone();
                        tokio::spawn(async move {
                            let _ = tx.send(RawEvent::GossipMessage {
                                author: None,
                                topic,
                                bytes,
                                held: true,
                            }).await;
                        });
                    }
                    SwarmCommand::Subscribe { topic } => {
                        let _ = swarm_tx.send(SwarmCommand::Subscribe { topic }).await;
                    }
                    SwarmCommand::Unsubscribe { topic } => {
                        let _ = swarm_tx.send(SwarmCommand::Unsubscribe { topic }).await;
                    }
                    SwarmCommand::Shutdown => {
                        let _ = swarm_tx.send(SwarmCommand::Shutdown).await;
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}

pub enum SwarmCommand {
    Publish {
        topic: String,
        payload: Vec<u8>,
        content_type: u16,
    },
    /// Publish, and hand back the envelope as sent (for a mailbox deposit).
    /// None when it wasn't sent (sealed-only and the topic has no key).
    PublishHeld {
        topic: String,
        payload: Vec<u8>,
        content_type: u16,
        reply: tokio::sync::oneshot::Sender<Option<Vec<u8>>>,
    },
    /// An envelope fetched from a mailbox, to receive as if it came live.
    InjectHeld {
        topic: String,
        bytes: Vec<u8>,
    },
    Subscribe {
        topic: String,
    },
    Unsubscribe {
        topic: String,
    },
    Shutdown,
}

pub(crate) enum RawEvent {
    GossipMessage {
        /// The transport peer that published it (gossipsub `source`), as
        /// opposed to the neighbour that forwarded it.
        author: Option<libp2p::PeerId>,
        topic: String,
        bytes: Vec<u8>,
        /// Fetched from a relay mailbox rather than received live: may be
        /// days old (`signed::verify_held`).
        held: bool,
    },
    PeerDiscovered {
        peer_id: libp2p::PeerId,
        addr: libp2p::Multiaddr,
    },
    PeerLost {
        peer_id: libp2p::PeerId,
    },
    RelayAccepted {
        relay_addr: libp2p::PeerId,
    },
    RelayLost,
    PublishFailed {
        topic: String,
    },
}

#[derive(Clone)]
pub struct ZRPHandle {
    cmd_tx: mpsc::Sender<SwarmCommand>,
    seals: Arc<Seals>,
    blob_manager: Option<Arc<BlobManager>>,
    debug_log_endpoint: Option<String>,
    store: Arc<dyn GhostStore>,
}

impl ZRPHandle {
    pub async fn publish(&self, topic: String, payload: Vec<u8>) {
        self.publish_typed(topic, payload, 0).await;
    }

    pub async fn publish_typed(&self, topic: String, payload: Vec<u8>, content_type: u16) {
        let _ = self
            .cmd_tx
            .send(SwarmCommand::Publish {
                topic,
                payload,
                content_type,
            })
            .await;
    }

    pub async fn send(
        tx: tokio::sync::mpsc::Sender<SwarmCommand>,
        topic: String,
        payload: Vec<u8>,
    ) {
        Self::send_typed(tx, topic, payload, 0).await;
    }

    pub async fn send_typed(
        tx: tokio::sync::mpsc::Sender<SwarmCommand>,
        topic: String,
        payload: Vec<u8>,
        content_type: u16,
    ) {
        let _ = tx
            .send(SwarmCommand::Publish {
                topic,
                payload,
                content_type,
            })
            .await;
    }

    pub async fn subscribe(&self, topic: String) {
        let _ = self.cmd_tx.send(SwarmCommand::Subscribe { topic }).await;
    }

    pub async fn unsubscribe(&self, topic: String) {
        let _ = self.cmd_tx.send(SwarmCommand::Unsubscribe { topic }).await;
    }

/// Publish on `topic` and also leave the envelope, exactly as sent (signed,
    /// and sealed if the topic is), in `mailbox_id` on the relay at
    /// `grpc_addr`, for a recipient who is offline now. Receivers check it like
    /// any live message when they fetch it (`fetch_held`). The live copy goes
    /// out even if the deposit fails.
    pub async fn publish_held(
        &self,
        topic: String,
        payload: Vec<u8>,
        content_type: u16,
        mailbox_id: &str,
        grpc_addr: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (reply, envelope) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(SwarmCommand::PublishHeld { topic, payload, content_type, reply })
            .await
            .map_err(|_| "mesh stopped")?;
        let envelope = envelope.await.map_err(|_| "mesh stopped")?.ok_or("not sent")?;
        let tag = crate::push::tag(self.store.as_ref(), mailbox_id);
        crate::mailbox::deposit(self.store.as_ref(), grpc_addr, mailbox_id, envelope, tag).await?;
        Ok(())
    }

    /// Fetch what `mailbox_id` holds that this device hasn't read, and receive
    /// it as if it had arrived on `topic` live: opened if the topic is sealed,
    /// signature-checked, given to handlers once. Returns how many came.
    pub async fn fetch_held(
        &self,
        topic: &str,
        mailbox_id: &str,
        grpc_addr: &str,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let cursor_key = format!("zkrp/mailbox/cursor/{mailbox_id}");
        let mut after = self
            .store
            .get(&cursor_key)
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();
        let mut count = 0;
        loop {
            let (held, more) =
                crate::mailbox::fetch(self.store.as_ref(), grpc_addr, mailbox_id, &after).await?;
            let Some(last) = held.last() else { break };
            after = last.msg_id.clone();
            for h in held {
                count += 1;
                let _ = self
                    .cmd_tx
                    .send(SwarmCommand::InjectHeld { topic: topic.to_string(), bytes: h.ciphertext })
                    .await;
            }
            self.store.set(&cursor_key, after.clone().into_bytes());
            if !more {
                break;
            }
        }
        Ok(count)
    }

    /// This context's sealed topics (see `crate::sealed`).
    pub fn seals(&self) -> &Seals {
        &self.seals
    }

    /// Seal `topic` with `key` and subscribe to it.
    pub async fn subscribe_sealed(&self, topic: String, key: [u8; 32]) {
        self.seals.insert(topic.clone(), key);
        self.subscribe(topic).await;
    }

    /// Unsubscribe from `topic` and forget its key.
    pub async fn unsubscribe_sealed(&self, topic: String) {
        self.seals.remove(&topic);
        self.unsubscribe(topic).await;
    }

    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(SwarmCommand::Shutdown).await;
    }

    pub async fn discover_relays(
        grpc_addr: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut client = RelayClient::connect(grpc_addr).await?;
        let relays = client.list_relays().await?;

        relays
            .iter()
            .for_each(|relay| tracing::info!("{:?}", relay));

        Ok(relays.into_iter().map(|r| r.multiaddr).collect())
    }

    pub async fn upload_blob(&self, data: Vec<u8>) -> Result<String, crate::blob::BlobError> {
        self.blob_manager.as_ref().unwrap().upload(data).await
    }

    pub async fn retrieve_blob(&self, blob_id: &str) -> Result<Vec<u8>, crate::blob::BlobError> {
        self.blob_manager.as_ref().unwrap().retrieve(blob_id).await
    }

    pub fn blob_storage_bytes(&self) -> u64 {
        self.blob_manager.as_ref().unwrap().storage_usage()
    }

    pub fn evict_old_blobs(&self, max_age_secs: u64) {
        self.blob_manager.clone().unwrap().evict_old(max_age_secs);
    }

    pub async fn upload_debug_logs(
        &self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = self
            .debug_log_endpoint
            .as_ref()
            .ok_or("no debug log endpoint configured")?;
        let data = crate::debug::global_buffer()
            .map(|b| b.snapshot())
            .unwrap_or_default();
        let resp = crate::debug::upload(endpoint, data, self.store.as_ref()).await?;
        Ok(resp.log_id)
    }
}

pub struct ZRPContext {
    codecs: Arc<Mutex<HashMap<u16, Codec>>>,
    handlers: Arc<Mutex<HashMap<String, Arc<dyn EventHandler>>>>,
    store: Arc<dyn GhostStore>,
    debug_log_endpoint: Option<String>,
    blob_manager: Option<Arc<BlobManager>>,
    seals: Arc<Seals>,
}

impl ZRPContext {
    /// Sealed topics, to configure before `start` (e.g. sealed-only mode).
    /// Shared with the handle `start` returns.
    pub fn seals(&self) -> Arc<Seals> {
        Arc::clone(&self.seals)
    }

    pub async fn register_codec(&mut self, id: u16, codec: Codec) {
        self.codecs.lock().await.insert(id, codec);
    }

    pub fn set_debug_log_endpoint(&mut self, endpoint: impl Into<String>) {
        self.debug_log_endpoint = Some(endpoint.into());
    }

    pub async fn register_handler(
        &mut self,
        name: impl Into<String>,
        handler: impl EventHandler + 'static,
    ) {
        self.handlers
            .lock()
            .await
            .insert(name.into(), Arc::new(handler));
    }

    pub fn remove_handler(&mut self, name: &str) {
        let mut handlers = self.handlers.blocking_lock();
        handlers.remove(name);
    }

    pub fn blob_manager(&self) -> Option<Arc<BlobManager>> {
        self.blob_manager.clone()
    }

    pub async fn start<B, F>(
        self,
        identity: crate::identity::NodeIdentity,
        relay_addrs: Option<Vec<String>>,
        grpc_relays: Option<Vec<String>>,
        transport_config: crate::transport::TransportConfig,
        make_behavior: F,
    ) -> Result<ZRPHandle, Box<dyn std::error::Error>>
    where
        B: crate::traits::MeshBehaviour + Send + 'static,
        B::ToSwarm: Send,
        F: FnOnce(&libp2p::identity::Keypair, libp2p::relay::client::Behaviour) -> B,
    {
        let mut relays = relay_addrs.unwrap_or_default();
        let debug_log_endpoint = self.debug_log_endpoint.clone();

        if let Some(grpc) = grpc_relays {
            for grpc_addr in grpc {
                match ZRPHandle::discover_relays(&grpc_addr).await {
                    Ok(mut relay) => relays.append(&mut relay),
                    Err(e) => tracing::info!("Failed to discover relays: {}", e),
                }
            }
        }

        if relays.is_empty() {
            tracing::warn!("no relays given or discovered; only direct peers are reachable");
        }

        // `identity` is the long-term account. The swarm gets a fresh
        // transport identity every launch, so the peer id relays see changes
        // each session and never matches the account apps exchange.
        let account = identity.keypair.clone();
        let transport = crate::identity::NodeIdentity::generate();
        tracing::info!(
            "account {} using fresh transport identity {}",
            identity.peer_id_string(),
            transport.peer_id_string()
        );

        let (crypto_tx, crypto_rx) = mpsc::channel::<SwarmCommand>(32);
        let (swarm_tx, swarm_rx) = mpsc::channel::<SwarmCommand>(32);
        let (raw_tx, raw_rx) = mpsc::channel::<RawEvent>(64);
        let raw_tx_inject = raw_tx.clone();

        let store_handle = ZRPHandle {
            cmd_tx: crypto_tx.clone(),
            seals: Arc::clone(&self.seals),
            blob_manager: None,
            debug_log_endpoint: debug_log_endpoint.clone(),
            store: Arc::clone(&self.store),
        };

        let blob_manager = Arc::new(BlobManager::new(
            Arc::clone(&self.store),
            store_handle,
            identity.peer_id_string(),
        ));

        let swarm = libp2p::SwarmBuilder::with_existing_identity(transport.keypair)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_quic()
            .with_dns_config(ResolverConfig::cloudflare(), ResolverOpts::default())
            .with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)?
            .with_behaviour(|key, relay_client| make_behavior(key, relay_client))?
            .with_swarm_config(|cfg| {
                cfg.with_idle_connection_timeout(transport_config.idle_connection_timeout)
            })
            .build();

        tokio::spawn(swarm_task::<B>(
            swarm,
            relays,
            transport_config,
            swarm_rx,
            raw_tx,
            Arc::clone(&self.store),
        ));

        let handlers = Arc::clone(&self.handlers);
        let codecs = Arc::clone(&self.codecs);
        tokio::spawn(crypto_task(
            raw_rx,
            crypto_rx,
            swarm_tx,
            crypto_tx.clone(),
            handlers,
            codecs,
            Arc::clone(&self.store),
            Arc::clone(&blob_manager),
            account,
            Arc::clone(&self.seals),
            raw_tx_inject,
        ));

        let _ = crypto_tx
            .send(SwarmCommand::Subscribe {
                topic: crate::signed::HELLO_TOPIC.to_string(),
            })
            .await;

        Ok(ZRPHandle {
            cmd_tx: crypto_tx,
            seals: Arc::clone(&self.seals),
            blob_manager: Some(blob_manager),
            debug_log_endpoint,
            store: Arc::clone(&self.store),
        })
    }

    pub fn with_store(store: Arc<dyn GhostStore>) -> Self {
        let mut codecs = HashMap::new();
        codecs.insert(0x0001, Codec::text());
        Self {
            codecs: Arc::new(Mutex::new(codecs)),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            store,
            blob_manager: None,
            debug_log_endpoint: None,
            seals: Arc::default(),
        }
    }
}

impl Default for ZRPContext {
    fn default() -> Self {
        let mut codecs = HashMap::new();

        codecs.insert(0x0001, Codec::text());

        Self {
            codecs: Arc::new(Mutex::new(codecs)),
            handlers: Arc::new(Mutex::new(HashMap::new())),
            store: Arc::new(crate::store::MemoryStore::new()),
            blob_manager: None,
            debug_log_endpoint: None,
            seals: Arc::default(),
        }
    }
}

impl Zeroize for ZRPContext {
    fn zeroize(&mut self) {}
}

impl Drop for ZRPContext {
    fn drop(&mut self) {
        self.zeroize();
    }
}
