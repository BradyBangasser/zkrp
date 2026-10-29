# Deploying zkrp

zkrp runs as three images, built from one [`Dockerfile`](../Dockerfile) and
published by CI for amd64 and arm64:

| Image | What |
| --- | --- |
| `ghcr.io/bradybangasser/zkrp-relay` | libp2p circuit relay, admission, gRPC control plane |
| `ghcr.io/bradybangasser/zkrp-auth` | enrollment: magic link + App Attest, blind credentials |
| `ghcr.io/bradybangasser/zkrp-caddy` | Caddy with the Cloudflare DNS plugin and zkrp's configs |

Two kinds of host run them:

- **The core host** ([`compose.yml`](../compose.yml) at the repository root):
  auth and Postgres behind Caddy. One per deployment.
- **Relay hosts** ([`relay/compose.yml`](relay/compose.yml)): a relay behind
  Caddy. As many as you like, in as many regions; each needs only that file
  and two env files, no checkout of this repository. One can also share the
  core host ([below](#a-relay-on-the-core-host)).

Images are tagged `latest` (main), the commit SHA, and the version for a `v*`
tag (`0.5.0`). They build only when something in them changes
(`.github/workflows/images.yml`), so a docs-only push never restarts a relay.

## Everything on one machine, for development

[`local/compose.yml`](local/compose.yml) runs the whole system from this
checkout: Postgres, SeaweedFS standing in for S3, auth, a relay, and
Prometheus.

```sh
scripts/local-up.sh              # first run makes keys; builds and starts
scripts/local-up.sh --lan        # reachable from a phone on this network
docker compose -f deploy/local/compose.yml logs -f relay auth
docker compose -f deploy/local/compose.yml down     # stop (add -v to wipe the data)
```

- gRPC is plaintext on `http://<LOCAL_IP>:9001` (relay) and `:9002` (auth),
  with no Caddy. Point the app at it with `GRPC_ADDR=http://<LOCAL_IP>:9001`
  in its Xcode scheme.
- Magic links aren't mailed: auth logs them (`logs -f auth`). App Attest runs
  in `development` mode, so enrollment needs a development build on a real
  iPhone.
- The first run copies the issuer key to `deploy/local/secrets/issuer-key`,
  so the FratRat test CLI can give itself a credential:
  `DEV_ISSUER_KEY=…/deploy/local/secrets/issuer-key cargo run -p ghost-cli`.
- With podman instead of Docker:
  `DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock scripts/local-up.sh`.
- Nothing here is for production: the passwords are fixed and the keys are
  throwaway.

## Hosting: AWS and Hetzner

Relays need raw TCP and UDP on a stable public address (libp2p, QUIC, circuit
relaying), so everything runs on plain VMs: EC2 or Hetzner Cloud, amd64 or
arm64, with Docker (or podman's Docker-compatible socket). Platforms that
front apps with shared anycast proxies (Fly.io and similar) don't work: a
reconnect can land on a different machine with a different peer id, and
QUIC/UDP passthrough is limited or absent.

For every VM:

- **Stable address.** An Elastic IP (AWS) or a Primary IP (Hetzner), so the
  address survives stop/start. DNS and `RELAY_PUBLIC_ADDRS` point at it.
- **Firewall** (security group / Hetzner Cloud Firewall), inbound:

  | Port | Protocol | Host | Why |
  | --- | --- | --- | --- |
  | 443 | TCP | all | gRPC over TLS (Caddy) |
  | 80 | TCP | all | ACME HTTP challenge. Not needed when only DNS-01 is used. |
  | 9000 | TCP + UDP | relays | libp2p (TCP and QUIC) |
  | 5432 | TCP | core | Postgres, **only from the relays' private addresses** |

  SSH from your own address only. Nothing else: gRPC, metrics and `/healthz`
  stay on each host's compose network.
- **AWS access.** Relays need S3 (blobs, mailboxes, logs); auth needs SES and
  KMS. On EC2 give the instance a role and leave the keys empty; on Hetzner,
  use an IAM user limited to exactly those.
- **Mailbox expiry.** Messages held for offline devices live under `mb/` in
  the mailbox bucket (`MAILBOX_BUCKET`, default `BLOB_BUCKET`). Add an S3
  lifecycle rule expiring the `mb/` prefix after 8 days: clients look back 7
  days, and nothing should be kept longer than that. Push registrations live
  under `push/` in the same bucket: give it the same rule.
- **Private network** between the core and relay hosts (Hetzner private
  network, VPC peering, or WireGuard across providers), for the relay
  registry in Postgres. Without it, each relay lists only itself.

## The core host

[`compose.yml`](../compose.yml): Postgres, auth, Caddy and an updater.
Caddy's config is `CADDY_ROLE` in `caddy.env`: `core` here (relay hosts
default to `relay`).

| Public address | Caddy → | For |
| --- | --- | --- |
| `https://<AUTH_DOMAIN>` | `h2c://auth:9002` | enrollment (apps) |

1. **DNS**: a grey-cloud (DNS only) `A`/`AAAA` record for `AUTH_DOMAIN` at the
   host's address.
2. **Check out the repository** (the core host builds nothing, but the
   compose file, the Postgres init scripts and the env templates live here):
   ```sh
   sudo git clone https://github.com/BradyBangasser/zkrp /opt/zkrp && cd /opt/zkrp
   ```
3. **Settings**: copy and fill in the three env files.
   ```sh
   for f in postgres auth caddy; do cp deploy/env/$f.env.example deploy/env/$f.env; done
   ```
   `DATABASE_URL` in `auth.env` must use `AUTH_DB_PASSWORD` from
   `postgres.env`; `caddy.env` needs `CADDY_ROLE=core`. In `.env` (read by compose itself), set `POSTGRES_BIND` to
   the host's private-network address so relays can reach the registry.
4. **Keys**, once:
   ```sh
   docker compose run --rm --no-deps auth keygen /secrets
   ```
   This writes `token-key`, `issuer-key`, `nullifier-key` and
   `community-key` into the `auth-secrets` volume and prints
   `ISSUER_PUBLIC_KEY`. It only adds missing keys and never overwrites one, so
   run it again after an upgrade that needs a new key (auth names the missing
   key and refuses to start). Put the public key in every relay's `relay.env`
   and in FratRat's `Zkrp.xcconfig`. **Back up `issuer-key`**: losing it
   invalidates every credential. `community-key` derives every school's
   community keys: whoever holds it can read what those communities seal.
   ```sh
   docker compose run --rm --no-deps --entrypoint cat auth /secrets/issuer-key
   ```
5. **Push** (optional). In the Apple developer account, create an APNs key
   (Keys → +, Apple Push Notifications service) and note its key id and your
   team id. Then:
   ```sh
   cp deploy/env/push.env.example deploy/env/push.env   # key id, team id, bundle id
   docker compose run --rm --no-deps push keygen /secrets   # prints PUSH_PUBLIC_KEY
   docker compose run --rm --no-deps -T --entrypoint sh push \
     -c 'cat > /secrets/apns-key.p8' < AuthKey_XXXXXXXXXX.p8
   ```
   Set `PUSH_BIND` in `.env` to the private-network address (like
   `POSTGRES_BIND`), and on every relay `PUSH_URL=http://<that address>:9003`
   (`http://push:9003` for a relay on this host) and `PUSH_PUBLIC_KEY`. The
   push service sees device tokens but no mailbox ids; relays see mailbox ids
   but only sealed tokens. Run both by the same operator and they could be
   correlated by timing, so neither logs either.
6. **Start**: `docker compose up -d`. Prometheus too:
   `docker compose --profile monitoring up -d` (on `127.0.0.1:9090`).

On first start Postgres runs `db/init/00-init.sh`, creating two databases with
separate roles:

| Database   | Role    | Rights                                              |
| ---------- | ------- | --------------------------------------------------- |
| `auth`     | `auth`  | `INSERT` on `nullifier` only, no `SELECT`           |
| `registry` | `relay` | read/write `relay`; no access to `auth`             |

### A relay on the core host

A small deployment can run its first relay on the core host, from the same
checkout: [`compose.relay.yml`](../compose.relay.yml) is its own compose
project (`zkrp-relay`), with the relay and an updater for it. It runs no
Caddy: the core's Caddy already holds ports 80 and 443 and fronts the relay
too. The relay joins the core's `zkrp-relays` network, where that Caddy
reaches its API and it reaches Postgres.

Each command names its file, so nothing in the shared `.env` decides what
runs. `scripts/setup-core-relay.sh` asks for the settings below and writes
both env files (`--start` also starts everything); by hand:

1. **Caddy**: in `deploy/env/caddy.env`, `CADDY_ROLE=core-relay` and
   `RELAY_DOMAIN` (or `core-relay-bootstrap`, plus `BOOTSTRAP_DOMAIN` and
   `CLOUDFLARE_API_TOKEN`), then `docker compose up -d`. This also creates
   the `zkrp-relays` network, so the core starts first.
2. **Relay**: copy and fill in `deploy/env/relay.env` as for a
   [relay host](#relay-hosts), with the core's Postgres by its service name:
   ```sh
   cp deploy/env/relay.env.example deploy/env/relay.env
   # DATABASE_URL=postgres://relay:<RELAY_DB_PASSWORD>@postgres/registry
   docker compose -f compose.relay.yml up -d       # relay + updater
   docker compose -f compose.relay.yml logs -f relay
   ```
3. **Firewall**: also open 9000 TCP and UDP on the core host.

The core's updater looks after auth and Caddy, the relay's after the relay.

## Relay hosts

[`relay/compose.yml`](relay/compose.yml): the relay, Caddy
(`CADDY_ROLE=relay`) and an updater.

| Public address | → | For |
| --- | --- | --- |
| `https://<RELAY_DOMAIN>` | Caddy → `h2c://relay:9001` | the relay's gRPC API |
| `https://<RELAY_DOMAIN>/healthz` | Caddy → `relay:9466` | DNS health monitors |
| `<RELAY_DOMAIN>:9000` TCP/UDP | relay | libp2p |

1. **DNS**: a grey-cloud `A`/`AAAA` record for the relay's name (for example
   `relay.a.fsn1.eu.infra.zkrp.net`) at its address.
2. **Files**: a directory with the compose file and two env files, no
   checkout needed:
   ```sh
   sudo mkdir -p /opt/zkrp-relay && cd /opt/zkrp-relay
   base=https://raw.githubusercontent.com/BradyBangasser/zkrp/main/deploy
   sudo curl -fsSLo compose.yml $base/relay/compose.yml
   sudo curl -fsSLo relay.env   $base/env/relay.env.example
   sudo curl -fsSLo caddy.env   $base/env/caddy.env.example
   ```
3. **Settings**: in `relay.env`, set `RELAY_PUBLIC_ADDRS`, `PUBLIC_GRPC_URL`,
   `ISSUER_PUBLIC_KEY`, `AUTH_URL`, the S3 buckets and `CF_DOMAIN`, and
   `DATABASE_URL` for the registry (the core's private address). In
   `caddy.env`, set `RELAY_DOMAIN`.
4. **Registry login** while the images are private: `docker login ghcr.io`
   with a token that has `read:packages`. (Or make the packages public in
   their GitHub settings.)
5. **Start**: `docker compose up -d`, then check:
   ```sh
   docker compose logs -f relay              # "zkrp-relay 0.5.0+<sha>", "Relay PeerID: ..."
   curl https://<RELAY_DOMAIN>/healthz       # ok
   ```
6. **Point clients at it**: add its address to the `bootstrap.zkrp.net`
   records (below). Relays in the registry show up in every other relay's
   `ListRelays` within 15 seconds.

### Updates

The `updater` service in both compose files pulls new images every five
minutes and recreates only what changed (never Postgres, which you upgrade
deliberately, and never itself). A relay update restarts the relay, and every
connected client drops and reconnects within seconds. To choose when that
happens:

- pin `TAG=<commit sha or version>` in the project's `.env`, or
- run without the updater (`docker compose up -d relay caddy`) and use
  [`systemd/zkrp-update@.timer`](systemd/zkrp-update@.timer) or update by hand
  (`docker compose pull && docker compose up -d`).

The updater drives the host's Docker through its socket, which is full control
of Docker on that machine. With podman, set `DOCKER_SOCKET` (and
`DOCKER_CONFIG` for a `sudo` login) in `.env`.

## TLS

gRPC is only ever public over TLS. Caddy gets and renews Let's Encrypt
certificates for its names and speaks cleartext HTTP/2 (h2c) to the service
on the compose network. Its configs are baked into the `zkrp-caddy` image
(`deploy/caddy/`); `CADDY_ROLE` picks one: `core`, `relay`,
`relay-bootstrap`, or for a [relay on the core host](#a-relay-on-the-core-host)
`core-relay` and `core-relay-bootstrap`. Clients use `https://` URLs; libghost verifies them
against the bundled Mozilla roots. libp2p on port 9000 doesn't need this:
it is already encrypted with Noise.

## bootstrap.zkrp.net: one name, nearest relay

Apps ship `bootstrap.zkrp.net` as their first bootstrap host. Getting a phone to
its nearest relay happens in two layers:

1. **DNS gets it to some healthy relay.** Only one is needed: it returns the
   full relay list.
2. **The app picks the nearest.** libghost times a TCP connect to every relay
   from the device itself and orders them nearest first (load breaks
   near-ties). That measures the real network path, which DNS can only guess
   at from the resolver's location, often far from the phone on cellular.

### DNS (Cloudflare)

Both options must be **DNS only** (grey cloud): Cloudflare's proxy would
terminate TLS itself and can't carry libp2p.

- **Free: one record per relay.** An `A`/`AAAA` record for `bootstrap.zkrp.net`
  at each relay, TTL 60. Resolvers return them all, and the gRPC client tries
  each address until one connects. Remove a relay's record when you retire it.
- **Load Balancing (paid add-on): nearest and health-checked.** One origin pool
  per region with the relay's address, an HTTPS monitor on `/healthz`
  expecting 200 with the Host header set to the relay's own name, and
  *proximity* steering. Attach it to `bootstrap.zkrp.net` in DNS-only mode.

### Relays: certificates for the shared name

Every relay behind `bootstrap.zkrp.net` needs its own certificate for it.
Let's Encrypt's HTTP and TLS-ALPN challenges would reach whichever relay DNS
picks, so they fail for most of them. DNS-01 through Cloudflare works for all
of them. On each such relay, in `caddy.env`:

```sh
CADDY_ROLE=relay-bootstrap
BOOTSTRAP_DOMAIN=bootstrap.zkrp.net
CLOUDFLARE_API_TOKEN=...   # Zone → DNS → Edit on zkrp.net
```

then `docker compose up -d caddy`, and check
`curl https://bootstrap.zkrp.net/healthz`.

## Settings

### Relay (`relay.env`)

| Variable             | Required | Notes |
| -------------------- | -------- | ----- |
| `RELAY_PUBLIC_ADDRS` | yes      | Address clients dial, without `/p2p`. Without it the relay grants no reservations and reports DEGRADED. |
| `PUBLIC_GRPC_URL`    | yes      | This relay's `https://` URL, handed to apps in `ListRelays`. |
| `ISSUER_PUBLIC_KEY`  | yes      | From `keygen` on the core host. |
| `AUTH_URL`           | yes      | Enrollment URL handed to apps by `GetNetworkConfig`. |
| `AWS_REGION`, `BLOB_BUCKET`, `LOG_BUCKET`, `CF_DOMAIN` | yes | S3 and the CDN that serves blobs. |
| `MAILBOX_BUCKET`     | no       | Held messages, under `mb/`. Default `BLOB_BUCKET`. Needs the `mb/` lifecycle rule. |
| `PUSH_URL`, `PUSH_PUBLIC_KEY` | no | The core's push service (private network) and its key. Both or neither; without them `Register` is refused and nothing is pushed. |
| `DATABASE_URL`       | no       | The shared registry. Empty: the relay lists only itself. |
| `EPOCH_SECS`         | no       | Must match auth. Default 30 days. |
| `RELAY_MESHES`, `REGION` | no   | Advertised in `Health`/`ListRelays`. |
| `ADMISSION`          | no       | `off` admits everyone. Development only. |
| `RUST_LOG`           | no       | Peer ids and addresses only appear at `debug`. |

The image sets `IDENTITY_PATH=/data/relay_identity.key` (on the `relay-data`
volume: the relay's peer id survives restarts), and the metrics (`9464`),
`/healthz` (`9466`) and gRPC (`9001`) listeners.

### Auth (`auth.env`)

| Variable | Notes |
| --- | --- |
| `DATABASE_URL` | `postgres://auth:<AUTH_DB_PASSWORD>@postgres/auth` |
| `ALLOWED_EMAIL_DOMAINS` | Who may enroll; subdomains included. |
| `MAILER`, `MAIL_FROM`, `VERIFY_LINK_BASE` | `ses` (or `log` in development); the magic link's base URL. |
| `APP_ATTEST_APP_ID`, `APP_ATTEST_ENV` | `<TEAM ID>.<bundle id>`; `production` or `development`. |
| `NULLIFIER_KMS_KEY_ID` | KMS HMAC key for nullifiers. Empty: the local `nullifier-key` secret. |
| `EPOCH_SECS` | Credential lifetime. Relays must match. |

Secrets are never environment variables: `keygen` writes them as files into
the `auth-secrets` volume, mounted at `/secrets`.

### Push (`push.env`)

| Variable | Notes |
| --- | --- |
| `APNS_KEY_ID`, `APNS_TEAM_ID` | The APNs key (`apns-key.p8` in the `push-secrets` volume) and team. |
| `APNS_TOPIC` | The app's bundle id. |
| `APNS` | `log`: log alerts instead of sending them (development). |
| `PUSH_ALERT_TITLE`, `PUSH_ALERT_BODY` | The alert's words; the default body is "New message". Alerts never carry content. |

`push keygen` writes `push-key` into the `push-secrets` volume. A new key
breaks every registration until devices register again (they do on their
next launch).

## Reports

User reports arrive through the anonymous log upload, filed under `report/`
in `LOG_BUCKET` (debug logs are under `log/`). They name the reported account
and, if the reporter chose, recent messages; never the reporter. On any relay
host:

```sh
docker compose exec relay zkrp-relay reports list          # oldest first
docker compose exec relay zkrp-relay reports show report/ab1/…
```

Decide how long to keep them and set an S3 lifecycle rule on `report/` to
match (and say so in the app's privacy policy).

## Health

`zkrp-relay healthcheck` and `zkrp-auth healthcheck` are the containers'
health checks. The relay's exits 0 for HEALTHY or DEGRADED: DEGRADED means no
public address or no registry, and a restart fixes neither. If the relay's
gRPC server dies, the whole process exits and Docker restarts it.

Metrics and SLO alerting: [`../docs/reliability.md`](../docs/reliability.md).
