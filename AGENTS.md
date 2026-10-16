# zkrp

zkrp is an anonymous, encrypted peer-to-peer network: devices enroll
anonymously, reach each other through libp2p relays, and leave
store-and-forward messages in mailboxes. [FratRat](../FratRat) (a dating app
for college students) is built on it and lives in its own repository.

## Non-negotiables

- **Anonymous from the infrastructure's point of view.** Relays and auth must
  learn as little as possible. Never add identity-linked logging, timestamps,
  per-user identifiers, linkable tags, or third-party identity providers
  (Clerk and the like were rejected). Peer ids and addresses are logged at
  `debug` only. Metrics carry counts, never identifiers.
- **Relays accept zero-knowledge proofs.** Admission is a BBS presentation
  (`crates/zkcred`) that reveals only the credential's epoch and is freshly
  randomized every time. No scoped pseudonyms: relays cannot rate-limit by
  credential, by design. Bound abuse per connection and by epoch length.
- **Auth persists only nullifiers.** `HMAC(pepper, epoch || slot || email)`,
  INSERT-only for the auth role. No email, key id, peer id, or timestamp, ever.
- **Two identities per device.** The long-term *account* key signs every mesh
  message and is what apps see; the libp2p *transport* key is new every
  launch, so relays can't link sessions. Attribute messages to the signing
  account, never to the gossipsub peer that forwarded them.
- **zkrp knows nothing about FratRat.** App strings (meshes, topics, buckets,
  labels) are configuration or an app namespace passed in, never constants.
- **Hosting is plain VMs (EC2, Hetzner).** Relays need raw TCP/UDP on stable
  per-machine IPs; anycast PaaS (Fly.io and similar) doesn't work.

## Layout

| Path | What |
| --- | --- |
| `crates/libghost` | client library (iOS via FratRat's `rust/ios`): mesh, `admit`, `enroll`, `directory`, `signed`, `grpc` |
| `crates/relay` | `zkrp-relay` binary: relay behaviour, admission gate, registry, gRPC services, `/healthz` |
| `crates/auth` | `zkrp-auth` binary: magic link (`token`), App Attest (`attest`), `nullifier`, issuance, `keygen` |
| `crates/zkcred` | BBS credentials; the admission context format both sides must build identically |
| `crates/telemetry` | metrics exporter, gRPC metrics layer, `version()` |
| `proto/` | `relay.proto` (RelayService, BlobStore, Logging), `mailbox.proto`, `auth.proto` |
| `db/` | Postgres init: separate `auth` and `registry` databases and roles |
| `Dockerfile`, `docker-bake.hcl` | images `relay`, `auth`, `caddy` from one compile |
| `compose.yml` | core host; `deploy/relay/compose.yml` relay hosts; `deploy/local/compose.yml` development |
| `deploy/` | env templates, Caddy configs (baked into the caddy image), systemd updater, the deployment guide |
| `docs/` | architecture, services, API reference, reliability |

## Working here

- Build and test: `cargo test --workspace` (protoc is vendored; no system
  protobuf). CI also runs `cargo clippy --workspace -- -D warnings` on amd64
  and arm64, and `scripts/check-compose.sh`.
- Run everything locally: `scripts/local-up.sh` (podman:
  `DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock`).
- Shared dependency versions live in the root `Cargo.toml`
  (`[workspace.dependencies]`); crates add features. Keep every `ark-*` crate
  on 0.4 (bbs_plus 0.25 depends on it).
- libghost's own content types are `0xF000` and up; apps own everything below.
- Images build only when what goes into them changes (`images.yml` path
  filters): a relay restart drops every connected client.
- Deployment is documented in `deploy/README.md`; keep it current when
  changing settings, ports, or images.

## Guidelines

- Ask when a request is ambiguous, especially anything touching privacy,
  credentials, or what the infrastructure can observe.
- Test end to end where it matters (relay + auth + client), not just units.
- Keep this file current as the project evolves.
