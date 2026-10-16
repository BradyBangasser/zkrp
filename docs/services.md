# Services

zkRP is a set of services that share the encrypted P2P substrate and a gRPC
control plane. Full request/response detail is in the gRPC API reference.

## auth service (`zrp.auth.v1`)

Anonymous enrollment and credential issuance. `AuthService` proves an email is
eligible and issues a blind credential without ever learning who enrolled; the
only state persisted is a set of 32-byte nullifiers. Deployed on its own port
(`AUTH_PORT`, default 9002) with its own Postgres database.

## controller (`zrp.relay.v1`, `zrp.mailbox.v1`)

A libp2p relay node plus a gRPC control plane, served on `GRPC_PORT`:

- **RelayService** - relay discovery (`ListRelays`), `Health`, `Stats`, and a
  streaming `WatchStats`. Open to everyone. `Register`/`Deregister` are refused:
  relays join through the shared registry instead.
- **BlobStore** - chunked, streaming blob upload to object storage. Downloads go
  through the CDN URL in the upload response. Requires an admission.
- **Logging** - client-streaming debug-log upload. Requires an admission.
- **MailboxService** - store-and-forward `Deposit`/`Fetch` so an offline device
  can be reached without the relay knowing who it is. Requires an admission.

## zkcred

The anonymous-credential library shared by auth (issues), relays (verify), and
clients (request and present). Not a service.

## libghost / ghost protocol

The encrypted P2P library and its wire envelope (`zrp.v0.ghost.GhostEnvelope`:
version, time nonce, payload). This is the transport the services ride on, not a
gRPC service.
