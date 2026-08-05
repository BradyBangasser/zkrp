# gRPC API reference

zkRP exposes several gRPC services. They fall into two deployments:

| Service | Package | Deployment | Default port |
| --- | --- | --- | --- |
| `AuthService` | `zrp.auth.v1` | auth service | `PORT` (Fly service, 9000) |
| `RelayService` | `zrp.relay.v1` | controller | `GRPC_PORT` (9001) |
| `BlobStore` | `zrp.relay.v1` | controller | `GRPC_PORT` (9001) |
| `Logging` | `zrp.relay.v1` | controller | `GRPC_PORT` (9001) |
| `MailboxService` | `zrp.mailbox.v1` | controller | `GRPC_PORT` (9001) |

## Transport

- gRPC over HTTP/2. The tonic servers speak h2c; transport security is handled at
  the deployment edge (Fly).
- No server reflection is enabled, so clients need the `.proto` files (under
  `proto/`) to generate stubs.
- No transport-level auth interceptor. Authorization is application-level: the
  `AuthService` issues anonymous credentials, and access to a mailbox is gated by
  possession of its capability id rather than by a bearer token on the wire.

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
