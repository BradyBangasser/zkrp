# RelayService

`package zrp.relay.v1`

Discovery, health, and lifecycle for the P2P relays. A client uses `ListRelays`
to find relays to connect to; relays call `Register`/`Deregister` to join and
leave; operators use `Health`, `Stats`, and `WatchStats` to observe them.

## Methods

| Method | Type | Request | Response |
| --- | --- | --- | --- |
| `ListRelays` | unary | `google.protobuf.Empty` | `RelayListResponse` |
| `Health` | unary | `google.protobuf.Empty` | `HealthResponse` |
| `Stats` | unary | `google.protobuf.Empty` | `StatsResponse` |
| `WatchStats` | server streaming | `WatchStatsRequest` | `stream StatsResponse` |
| `Register` | unary | `RegisterRequest` | `google.protobuf.Empty` |
| `Deregister` | unary | `DeregisterRequest` | `google.protobuf.Empty` |

## Messages

`RelayInfo`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `multiaddr` | `string` | 1 | libp2p multiaddr. |
| `peer_id` | `string` | 2 | Relay peer id. |
| `meshes` | `repeated string` | 3 | Gossipsub meshes served. |
| `region` | `string` | 4 | Deployment region. |
| `load` | `uint32` | 5 | Current load indicator. |
| `registered_at` | `uint64` | 6 | Unix seconds. |
| `port` | `uint32` | 7 | Relay port. |

`RelayListResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `relays` | `repeated RelayInfo` | 1 | Known relays. |
| `refresh_after_secs` | `uint64` | 2 | Advised re-fetch interval. |

`HealthResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `status` | `HealthStatus` | 1 | `HEALTHY` / `DEGRADED` / `UNHEALTHY`. |
| `peer_id` | `string` | 2 | Relay peer id. |
| `uptime_seconds` | `uint64` | 3 | Process uptime. |
| `meshes` | `repeated string` | 4 | Meshes served. |
| `version` | `string` | 5 | Build version. |

`RelayStats`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `connected_peers` | `uint32` | 1 | |
| `active_reservations` | `uint32` | 2 | |
| `messages_relayed` | `uint64` | 3 | |
| `bytes_relayed` | `uint64` | 4 | |
| `gossipsub_peers` | `uint32` | 5 | |
| `peers_per_mesh` | `map<string, uint32>` | 6 | Peers per gossipsub mesh. |
| `kademlia_peers` | `uint32` | 7 | |
| `cpu_usage` | `float` | 8 | |
| `memory_bytes` | `uint64` | 9 | |
| `uptime_seconds` | `uint64` | 10 | |

`StatsResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `current` | `RelayStats` | 1 | Latest snapshot. |
| `history` | `repeated RelayStats` | 2 | Recent snapshots. |
| `snapshot_interval_secs` | `uint64` | 3 | Snapshot cadence. |

`RegisterRequest` `{ RelayInfo info = 1; }` -
`DeregisterRequest` `{ string peer_id = 1; }` -
`WatchStatsRequest` `{ uint64 interval_secs = 1; }`

`HealthStatus` enum: `HEALTHY = 0`, `DEGRADED = 1`, `UNHEALTHY = 2`.

## Example

```bash
grpcurl -plaintext controller-host:9001 zrp.relay.v1.RelayService/ListRelays
grpcurl -plaintext -d '{"interval_secs":5}' \
  controller-host:9001 zrp.relay.v1.RelayService/WatchStats   # streams StatsResponse
```
