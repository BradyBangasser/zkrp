# Services

zkRP is a set of services that share the encrypted P2P substrate and a gRPC
control plane. Full request/response detail is in the gRPC API reference.

## auth service (`zrp.auth.v1`)

Anonymous enrollment and credential issuance. `AuthService` proves an email is
eligible and issues a blind credential without ever learning who enrolled; the
only state persisted is a set of 32-byte nullifiers. Deployed on its own port
(`PORT`).

## controller (`zrp.relay.v1`, `zrp.mailbox.v1`)

A libp2p relay node plus a gRPC control plane, served on `GRPC_PORT`:

- **RelayService** - relay discovery (`ListRelays`), `Health`, `Stats`, a
  streaming `WatchStats`, and relay `Register`/`Deregister`.
- **BlobStore** - chunked, streaming blob upload and download backed by object
  storage.
- **Logging** - client-streaming debug-log upload.
- **MailboxService** - store-and-forward `Deposit`/`Fetch` so an offline device
  can be reached without the relay knowing who it is.

## libghost / ghost protocol

The encrypted P2P library and its wire envelope (`zrp.v0.ghost.GhostEnvelope`:
version, time nonce, payload). This is the transport the services ride on, not a
gRPC service.
