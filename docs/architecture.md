# Architecture

zkRP is built for reachability without identity: devices can find each other,
relay through NATs, and receive messages while offline, without a central server
that knows who is talking to whom.

## Anonymous enrollment

The `auth` service issues blind credentials. A client proves an email is eligible
and receives a credential the server cannot link back to it. The server persists
only 32-byte nullifiers ("has this address enrolled?"), never an email, peer id,
or issued credential. Enrollment is stateless: state that would live server-side
is carried by the client in an opaque, MAC'd token. See AuthService in the API
reference.

## Encrypted P2P substrate

Nodes communicate over libp2p (gossipsub for pub/sub, Kademlia for discovery,
circuit relay for reachability). `libghost` wraps this, and messages ride inside
a `GhostEnvelope` (version, time nonce, opaque payload). Payloads are encrypted
end to end; relays route without reading them.

## Control plane

The `controller` runs a relay node and a gRPC control plane (`RelayService`,
`BlobStore`, `Logging`, `MailboxService`). This is how clients discover relays,
observe health and stats, move blobs, and use store-and-forward mailboxes.

## Store-and-forward

Because gossipsub is ephemeral, a message for an offline recipient is stored as
an opaque, end-to-end-encrypted blob under a rotating, capability-style
`mailbox_id` and fetched on reconnect. The relay stores only mailbox id to device
tokens, in memory and TTL'd, with no link to identity.
