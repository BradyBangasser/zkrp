//! Where the network is: relays (libp2p and gRPC), the auth service, the blob
//! CDN. Apps ship only a short bootstrap list of relay URLs; everything else is
//! learned from whichever relay answers and cached, so a dead bootstrap host or
//! a moved service doesn't need an app release.

use serde::{Deserialize, Serialize};

use crate::relay::RelayClient;
use crate::store::GhostStore;

const CACHE_KEY: &str = "zkrp/directory/v1";
/// Cap on candidates tried per refresh, so a long cached list can't stall
/// startup.
const MAX_ATTEMPTS: usize = 6;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DirectoryRelay {
    pub multiaddr: String,
    pub peer_id: String,
    pub region: String,
    pub load: u32,
    pub meshes: Vec<String>,
    /// Empty when the relay doesn't publish a gRPC endpoint.
    pub grpc_url: String,
    /// TCP connect time to the relay's libp2p address from this device, when
    /// it answered. Relays are ordered nearest first.
    pub rtt_ms: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Directory {
    pub relays: Vec<DirectoryRelay>,
    pub auth_url: String,
    pub blob_cdn_domain: String,
    pub epoch_secs: u64,
    /// Where push tokens are sealed to (`zkcred::sealed_box`); empty when the
    /// network has no push service.
    pub push_public_key: Vec<u8>,
    /// The gRPC URL that answered. Use it (or any relay's `grpc_url`) for
    /// blob, mailbox and log calls.
    pub source_url: String,
    pub fetched_at: u64,
    /// True when no candidate answered and this is the last good copy.
    #[serde(skip)]
    pub stale: bool,
}

impl Directory {
    /// gRPC URLs to reach relays by, the one that answered first.
    pub fn grpc_urls(&self) -> Vec<String> {
        let mut urls = vec![self.source_url.clone()];
        for r in &self.relays {
            if !r.grpc_url.is_empty() && !urls.contains(&r.grpc_url) {
                urls.push(r.grpc_url.clone());
            }
        }
        urls
    }
}

pub fn cached(store: &dyn GhostStore) -> Option<Directory> {
    let mut d: Directory = postcard::from_bytes(&store.get(CACHE_KEY)?).ok()?;
    d.stale = true;
    Some(d)
}

async fn fetch(url: &str) -> Result<Directory, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = RelayClient::connect(url).await?;
    let relays = client.list_relays().await?;
    let config = client.network_config().await?;
    Ok(Directory {
        relays: relays
            .into_iter()
            .map(|r| DirectoryRelay {
                multiaddr: r.multiaddr,
                peer_id: r.peer_id,
                region: r.region,
                load: r.load,
                meshes: r.meshes,
                grpc_url: r.grpc_url,
                rtt_ms: None,
            })
            .collect(),
        auth_url: config.auth_url,
        blob_cdn_domain: config.blob_cdn_domain,
        epoch_secs: config.epoch_secs,
        push_public_key: config.push_public_key,
        source_url: url.to_string(),
        fetched_at: crate::admit::now_unix(),
        stale: false,
    })
}

/// How long to wait for a relay before calling it unreachable.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// Relays this close in latency count as equally near; load breaks the tie.
const RTT_BUCKET_MS: u32 = 20;

/// `host:port` to dial for a libp2p TCP multiaddr.
fn tcp_target(multiaddr: &str) -> Option<String> {
    use libp2p::multiaddr::Protocol;
    let ma: libp2p::Multiaddr = multiaddr.parse().ok()?;
    let mut host = None;
    for p in ma.iter() {
        match p {
            Protocol::Ip4(ip) => host = Some(ip.to_string()),
            Protocol::Ip6(ip) => host = Some(format!("[{ip}]")),
            Protocol::Dns(h) | Protocol::Dns4(h) | Protocol::Dns6(h) => host = Some(h.to_string()),
            Protocol::Tcp(port) => return host.map(|h| format!("{h}:{port}")),
            _ => {}
        }
    }
    None
}

/// TCP connect time, which is what the app pays on every (re)connect.
async fn probe(multiaddr: &str) -> Option<u32> {
    let target = tcp_target(multiaddr)?;
    let start = std::time::Instant::now();
    match tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(&target)).await {
        Ok(Ok(_)) => Some(start.elapsed().as_millis().min(u32::MAX as u128) as u32),
        _ => None,
    }
}

/// Order nearest first: by latency bucket, then load. Unreachable last.
fn sort_relays(relays: &mut [DirectoryRelay]) {
    relays.sort_by_key(|r| match r.rtt_ms {
        Some(ms) => (0, ms / RTT_BUCKET_MS, r.load),
        None => (1, 0, r.load),
    });
}

/// Measure every relay concurrently, then sort.
async fn rank(relays: &mut [DirectoryRelay]) {
    let rtts = futures::future::join_all(relays.iter().map(|r| probe(&r.multiaddr))).await;
    for (r, rtt) in relays.iter_mut().zip(rtts) {
        r.rtt_ms = rtt;
    }
    sort_relays(relays);
}

/// Bootstrap URLs first, then relays learned on earlier runs.
fn candidates(bootstrap: &[String], cached: Option<&Directory>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let learned = cached.map(|d| d.grpc_urls()).unwrap_or_default();
    for url in bootstrap.iter().chain(learned.iter()) {
        let url = url.trim().to_string();
        if !url.is_empty() && !out.contains(&url) {
            out.push(url);
        }
    }
    out.truncate(MAX_ATTEMPTS);
    out
}

/// Ask candidates in order; cache and return the first full answer. Falls
/// back to the cached directory (marked stale) when nobody answers.
pub async fn refresh(store: &dyn GhostStore, bootstrap: &[String]) -> Option<Directory> {
    let previous = cached(store);
    for url in candidates(bootstrap, previous.as_ref()) {
        match fetch(&url).await {
            Ok(mut d) => {
                rank(&mut d.relays).await;
                if let Ok(bytes) = postcard::to_allocvec(&d) {
                    store.set(CACHE_KEY, bytes);
                }
                return Some(d);
            }
            Err(e) => tracing::warn!("network directory: {url} unavailable: {e}"),
        }
    }
    previous
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    fn dir(source: &str, relay_urls: &[&str]) -> Directory {
        Directory {
            relays: relay_urls
                .iter()
                .map(|u| DirectoryRelay {
                    multiaddr: String::new(),
                    peer_id: String::new(),
                    region: String::new(),
                    load: 0,
                    meshes: vec![],
                    grpc_url: u.to_string(),
                    rtt_ms: None,
                })
                .collect(),
            auth_url: "https://auth.example".into(),
            blob_cdn_domain: String::new(),
            epoch_secs: 0,
            push_public_key: vec![],
            source_url: source.into(),
            fetched_at: 0,
            stale: false,
        }
    }

    #[test]
    fn bootstrap_then_learned_without_duplicates() {
        let d = dir("https://a", &["https://a", "https://b", ""]);
        assert_eq!(
            candidates(&["https://boot".into(), "https://a".into()], Some(&d)),
            vec!["https://boot", "https://a", "https://b"]
        );
    }

    fn relay(name: &str, load: u32, rtt_ms: Option<u32>) -> DirectoryRelay {
        DirectoryRelay {
            multiaddr: String::new(),
            peer_id: name.into(),
            region: String::new(),
            load,
            meshes: vec![],
            grpc_url: String::new(),
            rtt_ms,
        }
    }

    #[test]
    fn nearest_first_load_breaks_ties_unreachable_last() {
        let mut rs = vec![
            relay("down", 0, None),
            relay("far", 0, Some(180)),
            relay("near-busy", 50, Some(12)),
            relay("near-idle", 3, Some(18)),
        ];
        sort_relays(&mut rs);
        let order: Vec<_> = rs.iter().map(|r| r.peer_id.as_str()).collect();
        assert_eq!(order, ["near-idle", "near-busy", "far", "down"]);
    }

    #[test]
    fn tcp_targets() {
        assert_eq!(tcp_target("/dns4/relay.example.net/tcp/9000/p2p/12D3KooWDz3MYD8ccTTVz6tjH4dNsvkfuegSjvsnm9akGi7hFPC4").as_deref(), Some("relay.example.net:9000"));
        assert_eq!(tcp_target("/ip6/::1/tcp/9000").as_deref(), Some("[::1]:9000"));
        assert_eq!(tcp_target("/ip4/1.2.3.4/udp/9000/quic-v1"), None);
    }

    #[tokio::test]
    async fn probes_a_listening_port() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        assert!(probe(&format!("/ip4/127.0.0.1/tcp/{port}")).await.is_some());
        drop(l);
        assert!(probe(&format!("/ip4/127.0.0.1/tcp/{port}")).await.is_none());
    }

    #[tokio::test]
    async fn falls_back_to_cache_when_nothing_answers() {
        let store = MemoryStore::new();
        assert_eq!(refresh(&store, &[]).await, None);
        let d = dir("http://127.0.0.1:1", &[]);
        store.set(CACHE_KEY, postcard::to_allocvec(&d).unwrap());
        let got = refresh(&store, &[]).await.unwrap();
        assert!(got.stale);
        assert_eq!(got.auth_url, "https://auth.example");
    }
}
