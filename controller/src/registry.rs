//! Shared relay registry, backed by Postgres (`db/registry.sql`).
//!
//! Every relay upserts a heartbeat row; `ListRelays` serves the other live
//! relays from an in-memory snapshot that the heartbeat loop refreshes. The
//! database is never on the request path, so a Postgres outage degrades
//! discovery to "just me" instead of failing it.
//!
//! Only relay infrastructure lives here. Nothing about clients.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::RwLock;

use crate::api::proto::RelayInfo;

const HEARTBEAT: Duration = Duration::from_secs(15);
/// A relay that has missed three heartbeats is no longer advertised.
const STALE_AFTER_SECS: i64 = 45;

#[derive(Clone)]
pub struct Registry {
    pool: Option<PgPool>,
    peers: Arc<RwLock<Vec<RelayInfo>>>,
    healthy: Arc<AtomicBool>,
}

/// What this relay advertises about itself.
pub struct SelfEntry {
    pub peer_id: String,
    pub multiaddr: String,
    pub grpc_url: String,
    pub region: String,
    pub meshes: Vec<String>,
}

impl Registry {
    /// A registry that only ever knows about this relay.
    pub fn standalone() -> Self {
        Self {
            pool: None,
            peers: Arc::new(RwLock::new(Vec::new())),
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Connects lazily: an unreachable database at boot is reported through
    /// `is_healthy`, not by refusing to start.
    pub fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy(database_url)?;
        Ok(Self {
            pool: Some(pool),
            peers: Arc::new(RwLock::new(Vec::new())),
            healthy: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Other live relays, as of the last successful heartbeat.
    pub async fn peers(&self) -> Vec<RelayInfo> {
        self.peers.read().await.clone()
    }

    /// Heartbeat forever. No-op without a database.
    pub async fn run(self, me: SelfEntry, load: Arc<AtomicU32>) {
        let Some(pool) = self.pool.clone() else {
            return;
        };
        let mut tick = tokio::time::interval(HEARTBEAT);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let result = async {
                upsert(&pool, &me, load.load(Ordering::Relaxed)).await?;
                live_peers(&pool, &me.peer_id).await
            }
            .await;
            match result {
                Ok(peers) => {
                    *self.peers.write().await = peers;
                    if !self.healthy.swap(true, Ordering::Relaxed) {
                        tracing::info!("relay registry reachable");
                    }
                }
                Err(e) => {
                    // Keep serving the last good snapshot.
                    if self.healthy.swap(false, Ordering::Relaxed) {
                        tracing::warn!("relay registry unreachable: {e}");
                    }
                }
            }
        }
    }

    /// Best-effort removal on graceful shutdown so clients stop being sent
    /// here immediately rather than after `STALE_AFTER_SECS`.
    pub async fn deregister(&self, peer_id: &str) {
        let Some(pool) = &self.pool else { return };
        let q = sqlx::query("DELETE FROM relay WHERE peer_id = $1").bind(peer_id);
        if let Err(e) = tokio::time::timeout(Duration::from_secs(3), q.execute(pool)).await {
            tracing::warn!("registry deregister timed out: {e}");
        }
    }
}

async fn upsert(pool: &PgPool, me: &SelfEntry, load: u32) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO relay (peer_id, multiaddr, region, meshes, load, grpc_url, heartbeat_at)
         VALUES ($1, $2, $3, $4, $5, $6, now())
         ON CONFLICT (peer_id) DO UPDATE SET
           multiaddr = EXCLUDED.multiaddr, region = EXCLUDED.region,
           meshes = EXCLUDED.meshes, load = EXCLUDED.load,
           grpc_url = EXCLUDED.grpc_url, heartbeat_at = now()",
    )
    .bind(&me.peer_id)
    .bind(&me.multiaddr)
    .bind(&me.region)
    .bind(&me.meshes)
    .bind(load as i32)
    .bind(&me.grpc_url)
    .execute(pool)
    .await?;
    Ok(())
}

/// peer_id, multiaddr, region, meshes, load, heartbeat (unix secs), grpc_url
type RelayRow = (String, String, String, Vec<String>, i32, i64, String);

async fn live_peers(pool: &PgPool, self_id: &str) -> Result<Vec<RelayInfo>, sqlx::Error> {
    let rows: Vec<RelayRow> = sqlx::query_as(
        "SELECT peer_id, multiaddr, region, meshes, load,
                extract(epoch FROM heartbeat_at)::bigint, grpc_url
         FROM relay
         WHERE peer_id <> $1 AND heartbeat_at > now() - make_interval(secs => $2)
         ORDER BY load ASC",
    )
    .bind(self_id)
    .bind(STALE_AFTER_SECS as f64)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(peer_id, multiaddr, region, meshes, load, seen, grpc_url)| {
            let port = port_of(&multiaddr);
            RelayInfo {
                multiaddr,
                peer_id,
                meshes,
                region,
                load: load.max(0) as u32,
                registered_at: seen.max(0) as u64,
                port,
                grpc_url,
            }
        })
        .collect())
}

fn port_of(multiaddr: &str) -> u32 {
    let mut parts = multiaddr.split('/');
    while let Some(p) = parts.next() {
        if p == "tcp" || p == "udp" {
            return parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::port_of;

    #[test]
    fn port_from_multiaddr() {
        assert_eq!(port_of("/dns4/relay.example/tcp/9000/p2p/12D3"), 9000);
        assert_eq!(port_of("/ip4/1.2.3.4/udp/9001/quic-v1"), 9001);
        assert_eq!(port_of("/ip4/1.2.3.4"), 0);
    }
}
