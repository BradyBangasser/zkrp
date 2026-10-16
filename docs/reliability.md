# Reliability

## Targets

- **SLO: 99.9%** availability per service. This is the internal target every
  service is built and measured against.
- **SLA: 99%** to customers. The gap between the 99.9% target and the 99%
  commitment is deliberate headroom, so a bad window does not immediately breach
  the customer contract.

## How it is measured

The relay and auth serve Prometheus metrics (`METRICS_ADDR`, default `:9464`
for the relay and `:9465` for auth). Every label comes from a fixed set, and no
metric carries a peer id, address, mailbox id, or email.

| Metric | Type | Labels |
| --- | --- | --- |
| `zkrp_grpc_requests_total` | counter | `service`, `method`, `code` |
| `zkrp_grpc_request_duration_seconds` | histogram | `service`, `method` |
| `zkrp_admissions_total` | counter | `transport` (`p2p`, `grpc`), `result` |
| `zkrp_peers_dropped_unadmitted_total` | counter | |
| `zkrp_connected_peers`, `zkrp_active_reservations`, `zkrp_active_circuits` | gauge | |
| `zkrp_relay_health_status` | gauge | 0 healthy, 1 degraded, 2 unhealthy |
| `zkrp_registry_healthy`, `zkrp_uptime_seconds` | gauge | |
| `zkrp_auth_links_total`, `zkrp_auth_enrollments_total` | counter | `result` |

A request counts against the SLO only when the server is at fault (`UNKNOWN`,
`INTERNAL`, `UNAVAILABLE`, `DATA_LOSS`, `DEADLINE_EXCEEDED`). Caller errors such
as `UNAUTHENTICATED` don't spend the budget. Availability over the window:

```
1 - sum(increase(zkrp_grpc_requests_total{code=~"UNKNOWN|INTERNAL|UNAVAILABLE|DATA_LOSS|DEADLINE_EXCEEDED"}[30d]))
  / sum(increase(zkrp_grpc_requests_total[30d]))
```

`monitoring/rules.yml` records this per service and alerts with multi-window
burn rates (fast burn pages, slow burn files a ticket), plus `up == 0` and a
relay stuck DEGRADED. `docker compose --profile monitoring up -d` runs
Prometheus with these rules.

A stream that fails after its first message reports its status in trailers,
which the metrics layer doesn't read. Such failures are counted as `OK`.

## Error budget

A 99.9% SLO allows roughly 43 minutes of downtime per 30-day window. Tracking
burn against that budget is what turns the SLO from a number in a README into
something operable: when the budget is healthy you ship, when it is nearly spent
you slow down and stabilize.
