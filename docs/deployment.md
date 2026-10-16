# Deployment

Everything ships as two images, built by CI on every push to `main` and every
`v*` tag:

- `ghcr.io/bradybangasser/zkrp-relay` — the relay and its gRPC control plane
- `ghcr.io/bradybangasser/zkrp-auth` — enrollment and credential issuance

`protoc` is vendored by the build scripts, so building needs only a Rust
toolchain (or Docker/Podman).

## Full stack with Compose

Postgres, auth, and one relay on a single host:

```bash
cp .env.example .env           # fill in
docker compose run --rm --no-deps --entrypoint /usr/local/bin/zkrp-auth \
  -v ./secrets:/out:z auth keygen /out
# copy the printed ISSUER_PUBLIC_KEY into .env
docker compose up -d --build
```

`keygen` writes `token-key`, `issuer-key`, and `nullifier-key` (0600) plus
`issuer-public-key` into `./secrets`, which is mounted read-only at
`/run/secrets`. Secrets never go in environment variables. Back up
`issuer-key`: losing it invalidates every credential when the epoch rolls over.

On first start, Postgres runs `db/init/00-init.sh`, which creates two databases
with separate roles:

| Database   | Role    | Rights                                              |
| ---------- | ------- | --------------------------------------------------- |
| `auth`     | `auth`  | `INSERT` on `nullifier` only, no `SELECT`           |
| `registry` | `relay` | read/write `relay`; no access to `auth`             |

## TLS

gRPC is only ever public over TLS. Caddy terminates it on 443 and gets and
renews Let's Encrypt certificates on its own (ports 80 and 443 must be
reachable), then talks HTTP/2 cleartext to the services on a private network
or loopback:

- **Compose**: the `caddy` service serves `RELAY_DOMAIN` and `AUTH_DOMAIN`
  (`deploy/Caddyfile`). The plaintext ports 9001/9002 are published on
  127.0.0.1 only.
- **Relay hosts**: `caddy.service` serves `RELAY_DOMAIN` and the relay binds
  gRPC to 127.0.0.1 (`GRPC_BIND`).

Clients use `https://` addresses. libghost verifies them against the bundled
Mozilla roots and warns whenever it is given a plaintext `http://` address.
libp2p traffic on port 9000 doesn't need this: it is already encrypted with
Noise.

## How apps find the network

Apps ship only a bootstrap list of relay hosts and the pinned issuer key. Every
relay answers `ListRelays` (each entry carries the relay's `grpc_url`) and
`GetNetworkConfig` (`AUTH_URL`, `CF_DOMAIN`, epoch length), and libghost caches
the last good answer on the device (`libghost::directory`). A new relay joins
by registering in the shared registry. Moving the auth service means changing
`AUTH_URL` on the relays. Neither needs an app release, and as long as one
bootstrap or previously seen relay answers, clients find the rest.

## bootstrap.zkrp.net: one name, nearest relay

Apps ship `bootstrap.zkrp.net` as their first bootstrap host. Getting a phone to
its nearest relay happens in two layers:

1. **DNS gets it to some healthy relay.** Only one is needed: it returns
   the full relay list.
2. **The app picks the nearest.** After fetching the list, libghost times
   a TCP connect to every relay from the device itself and orders them nearest
   first (load breaks near-ties). This uses the real network path, which DNS
   can only guess at from the resolver's location, and that's often far
   from the phone on cellular.

### DNS (Cloudflare)

Pick one. Both must be **DNS only** (grey cloud): Cloudflare's proxy would
terminate TLS itself and can't carry libp2p.

- **Free: one record per relay.** Add an `A`/`AAAA` record for
  `bootstrap.zkrp.net` pointing at each relay, TTL 60. Resolvers return them
  all, and the gRPC client tries each address until one connects, so a dead
  relay costs a moment, not a failure. Remove a relay's record when you
  retire it.
- **Load Balancing (paid add-on): nearest and health-checked.** Create one
  origin pool per region with the relay's address as the origin, a monitor
  of type HTTPS on path `/healthz`, expected code 200, with the Host header
  set to the relay's own name, and *proximity* steering (give each pool
  its coordinates). Attach the load balancer to `bootstrap.zkrp.net` in DNS-only
  mode. Dead relays drop out within one monitor interval.

### Relays: certificates for the shared name

Every relay behind `bootstrap.zkrp.net` needs its own certificate for it.
Let's Encrypt's HTTP and TLS-ALPN challenges would reach whichever relay DNS
picks, so they fail for most of them. Use DNS-01 through Cloudflare instead:

1. Create a Cloudflare API token with **Zone → DNS → Edit** on `zkrp.net`.
2. On each relay host:
   ```bash
   printf '%s' "$TOKEN" | sudo systemd-creds encrypt --name=cloudflare-token - \
     /etc/zkrp/credentials/cloudflare-token.cred
   sudo cp /opt/zkrp/repo/controller/bootstrap.caddy /etc/zkrp/caddy.d/
   sudo $EDITOR /etc/system.env   # BOOTSTRAP_DOMAIN=bootstrap.zkrp.net
                                  # CADDY_IMAGE=ghcr.io/bradybangasser/zkrp-caddy
   sudo $EDITOR /etc/systemd/system/caddy.service   # uncomment LoadCredentialEncrypted
   sudo systemctl daemon-reload && sudo systemctl restart caddy
   ```
   `zkrp-caddy` is stock Caddy plus the Cloudflare DNS plugin
   (`deploy/caddy/Containerfile`, built by CI).
3. Check it: `curl https://bootstrap.zkrp.net/healthz` should print `ok`.

In compose, the equivalent is `deploy/sites.d/bootstrap.caddy.example` plus
`BOOTSTRAP_DOMAIN`, `CADDY_IMAGE`, and `CLOUDFLARE_API_TOKEN` in `.env`.

## Hosting: AWS and Hetzner

Relays need raw TCP and UDP on a stable public address (libp2p, QUIC, circuit
relaying), so they run on plain VMs: EC2 or Hetzner Cloud. Platforms that front
apps with shared anycast proxies (Fly.io and similar) don't work here, because
a reconnect can land on a different machine with a different peer id, and
QUIC/UDP passthrough is limited or absent.

For every relay VM:

- **Stable address.** Attach an Elastic IP (AWS) or keep a Primary IP (Hetzner),
  so the address survives stop/start. `RELAY_PUBLIC_ADDRS`, `RELAY_DOMAIN`, and
  the `bootstrap` DNS records all point at it. Add `/ip6/...` entries to
  `RELAY_PUBLIC_ADDRS` if the VM has a public IPv6 address.
- **Firewall** (security group / Hetzner Cloud Firewall), inbound:

  | Port | Protocol | Why |
  | --- | --- | --- |
  | 9000 | TCP + UDP | libp2p (TCP and QUIC) |
  | 443 | TCP | gRPC over TLS (Caddy) |
  | 80 | TCP | ACME HTTP challenge. Not needed when only DNS-01 is used. |

  Everything else stays closed. gRPC (9001), metrics (9464) and `/healthz`
  (9466) are bound to 127.0.0.1 on relay hosts anyway.
- **Persistent identity.** `/var/lib/zkrp` holds the relay key. Keep it on the
  root volume or a persistent volume, never instance-store storage, or the
  relay's peer id changes when the VM moves.

The core host (Postgres, auth) can be either provider. Auth uses SES and KMS,
so it needs AWS credentials wherever it runs. On EC2, prefer an instance role
to static keys.

## Relay-only hosts

Relays in other regions run just the relay image under systemd and Podman:

```bash
sudo controller/tools/setup.sh <repo-url> <aws-key-id> <aws-secret>
sudo $EDITOR /etc/system.env   # RELAY_PUBLIC_ADDRS, ISSUER_PUBLIC_KEY, buckets
sudo systemctl restart relay
```

`relay.service` pulls `zkrp-relay:${IMAGE_TAG}`, keeps the relay identity in
`/var/lib/zkrp`, and has Podman kill the container if `zkrp-relay healthcheck`
fails. To join the shared registry, add the `database-url` credential (see the
comment in the unit).

## Relay settings

| Variable             | Required | Notes |
| -------------------- | -------- | ----- |
| `RELAY_PUBLIC_ADDRS` | yes      | Address clients dial, without `/p2p`. Without it the relay can't grant reservations and reports DEGRADED. |
| `ISSUER_PUBLIC_KEY`  | yes      | From `zkrp-auth keygen`. |
| `ADMISSION`          | no       | `off` admits everyone. Local development only. |
| `EPOCH_SECS`         | no       | Must match the auth service. Default 30 days. |
| `IDENTITY_PATH`      | no       | Default `/data/relay_identity.key` in the image. |
| `DATABASE_URL`       | no       | Shared registry; env var or `database-url` credential. |
| `RELAY_MESHES`, `REGION` | no   | Advertised in `Health`/`ListRelays`. |
| `RUST_LOG`           | no       | Peer ids and addresses only appear at `debug`. |
| `PUBLIC_GRPC_URL`    | no       | This relay's https URL for apps. Default `https://$RELAY_DOMAIN`. |
| `AUTH_URL`           | no       | Enrollment URL served to apps by `GetNetworkConfig`. |
| `HEALTH_ADDR`        | no       | Plain HTTP `/healthz` for DNS health monitors. Default `0.0.0.0:9466`. |
| `GRPC_BIND`          | no       | `127.0.0.1` behind a local TLS proxy. Default `0.0.0.0`. |
| `METRICS_ADDR`       | no       | Prometheus endpoint. Default `0.0.0.0:9464`; `127.0.0.1:9464` on host-network installs. |

## Health

`zkrp-relay healthcheck` calls `Health` on the local gRPC port. It exits 0 for
HEALTHY or DEGRADED: DEGRADED means no public address or no registry, and a
restart fixes neither. If the gRPC server dies the whole process exits, so the
supervisor restarts it instead of leaving a half-alive relay.

## Local development

```bash
cargo test --workspace
cargo run -p auth -- keygen ./secrets
# auth without SES or a real device (debug builds only):
MAILER=log APP_ATTEST_ENV=disabled CREDENTIALS_DIRECTORY=./secrets cargo run -p auth
```
