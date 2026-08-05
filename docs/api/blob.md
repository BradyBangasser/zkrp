# BlobStore

`package zrp.relay.v1`

Chunked upload and download of opaque blobs, backed by object storage. Uploads
stream chunks from the client and return an id and URL; downloads stream chunks
back.

## Methods

| Method | Type | Request | Response |
| --- | --- | --- | --- |
| `UploadBlob` | client streaming | `stream UploadChunk` | `UploadResponse` |
| `DownloadBlob` | server streaming | `DownloadRequest` | `stream BlobChunk` |

## Messages

| Message | Fields |
| --- | --- |
| `UploadChunk` | `bytes data = 1` |
| `UploadResponse` | `string blob_id = 1`, `string blob_url = 2` |
| `DownloadRequest` | `string blob_id = 1` |
| `BlobChunk` | `bytes data = 1` |

The client sends a sequence of `UploadChunk` messages and receives a single
`UploadResponse` when the stream closes. `DownloadBlob` returns a stream of
`BlobChunk` for the given `blob_id`. Blob contents are opaque to the store.
