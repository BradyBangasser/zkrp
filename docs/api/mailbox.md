# MailboxService

`package zrp.mailbox.v1`

Store-and-forward for offline devices. Because gossipsub is ephemeral and push is
deferred, a message for an offline recipient is stored as an opaque,
end-to-end-encrypted blob under a mailbox id and fetched on reconnect. The relay
cannot read the ciphertext; it only routes by mailbox id.

A `mailbox_id` is a rotating, opaque **capability**, shared only with the people
who should be able to reach you and never derived from a name, peer id, or
credential. Knowing the id is what grants the ability to deposit to and fetch
from it, so treat it like a secret. The relay stores only
`mailbox_id -> {device tokens, expiry}` in memory, TTL'd, never logged, with no
link to identity or the nullifier set. There is deliberately no `Notify` RPC: the
relay fires pushes itself when it routes a message, so there is no public endpoint
to spam a device or probe whether a mailbox is live.

`MailboxKind` enum (for the two lifecycles a mailbox id can have): `USER = 0` (a
rotating personal mailbox for your own devices), `PARTY = 1` (a shared mailbox for
one event).

## Methods

| Method | Type | Request | Response |
| --- | --- | --- | --- |
| `Deposit` | unary | `DepositRequest` | `DepositResponse` |
| `Fetch` | unary | `FetchRequest` | `FetchResponse` |

### Deposit

Stores one end-to-end-encrypted message under a mailbox id.

`DepositRequest`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `mailbox_id` | `string` | 1 | Rotating capability id. |
| `ciphertext` | `bytes` | 2 | Opaque; encrypted client-side to the pair or party. |

`DepositResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `msg_id` | `string` | 1 | Lexicographically sortable, so chronological. |

### Fetch

Returns messages after a cursor. There is no server-side delete, so one party
cannot drop another's undelivered mail; the recipient tracks its own cursor.

`FetchRequest`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `mailbox_id` | `string` | 1 | Capability id. |
| `after` | `string` | 2 | Last `msg_id` already held; empty = from the earliest retained. |
| `limit` | `uint32` | 3 | `0` = server default. |

`Envelope`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `msg_id` | `string` | 1 | |
| `ciphertext` | `bytes` | 2 | |
| `stored_at` | `uint64` | 3 | Unix millis, advisory. |

`FetchResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `envelopes` | `repeated Envelope` | 1 | |
| `has_more` | `bool` | 2 | More available past this page. |

> Note: `MailboxService` is defined in `proto/mailbox.proto` and implemented in
> the controller. Confirm it is registered in your controller build's gRPC
> `serve()` if you intend to expose it, alongside `BlobStore`, `Logging`, and
> `RelayService`.
