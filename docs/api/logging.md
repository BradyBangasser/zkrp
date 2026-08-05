# Logging

`package zrp.relay.v1`

Client-streaming upload of debug logs. Used by clients to ship a debug bundle to
the controller for support.

## Methods

| Method | Type | Request | Response |
| --- | --- | --- | --- |
| `UploadDebugLog` | client streaming | `stream UploadChunk` | `LogUploadResponse` |

## Messages

| Message | Fields |
| --- | --- |
| `UploadChunk` | `bytes data = 1` (shared with `BlobStore`) |
| `LogUploadResponse` | `string log_id = 1` |

The client streams `UploadChunk` messages and receives a single
`LogUploadResponse` with the stored `log_id` when the stream closes.
