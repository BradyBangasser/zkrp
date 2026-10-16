# gRPC API reference

zkRP exposes several gRPC services. They fall into two deployments:

| Service | Package | Deployment | Default port |
| --- | --- | --- | --- |
| `AuthService` | `zrp.auth.v1` | auth service | `AUTH_PORT` (9002) |
| `RelayService` | `zrp.relay.v1` | controller | `GRPC_PORT` (9001) |
| `BlobStore` | `zrp.relay.v1` | controller | `GRPC_PORT` (9001) |
| `Logging` | `zrp.relay.v1` | controller | `GRPC_PORT` (9001) |
| `MailboxService` | `zrp.mailbox.v1` | controller | `GRPC_PORT` (9001) |

## Transport

- gRPC over HTTP/2. The tonic servers speak h2c on loopback or a private
  network; Caddy terminates TLS in front of them (see the deployment guide).
  Clients use `https://` URLs.
- No server reflection is enabled, so clients need the `.proto` files (under
  `proto/`) to generate stubs.
- `BlobStore`, `Logging` and `MailboxService` require an anonymous admission
  (a zero-knowledge proof of an `AuthService` credential) in the
  `x-zkrp-admit-bin` metadata. `RelayService` and `AuthService` are open.
  Mailbox access is additionally gated by knowing the mailbox's capability id.

## Method paths

gRPC method paths follow `/{package}.{Service}/{Method}`, for example
`/zrp.auth.v1.AuthService/GetChallenge`. The `grpcurl` examples on each page use
these paths.

## Streaming at a glance

| RPC | Type |
| --- | --- |
| `RelayService.WatchStats` | server streaming |
| `BlobStore.UploadBlob`, `Logging.UploadDebugLog` | client streaming |
| `BlobStore.DownloadBlob` | server streaming |
| everything else | unary |

## Pages

- **Auth** - anonymous enrollment and credential issuance.
- **Relay** - relay discovery, health, stats, and registration.
- **Blob** - chunked blob upload and download.
- **Logging** - debug log upload.
- **Mailbox** - store-and-forward for offline devices.
