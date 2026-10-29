# Architecture

zkRP is built for reachability without identity: devices can find each other,
relay through NATs, and receive messages while offline, without a central server
that knows who is talking to whom.

## Anonymous enrollment

The `auth` service issues anonymous credentials (`zkcred`: BBS signatures on
BLS12-381). Enrollment is a magic link plus an Apple App Attest attestation:

1. `GetChallenge(email)` emails a link carrying an encrypted token (expiry,
   challenge, email) and returns the challenge to the app.
2. The app commits to a fresh random *link secret* and attests over
   `SHA-256("zkrp/attest/v1" || challenge || SHA-256(credential_request))`.
3. `Redeem` verifies the attestation, records one nullifier, and blind-signs
   `[link_secret, epoch]`. The issuer never sees the link secret. It also
   returns the **community keys** for the credential's two epochs:
   `HMAC(community-key, domain || epoch)` for the allowed email domain the
   address falls under. Members seal their community's traffic with them
   (below). Whoever holds auth's `community-key` can derive them all.

The server persists only nullifiers: `HMAC(pepper, epoch || slot || email)`,
with the pepper in KMS. There are 3 slots per address per epoch (reinstall, second
device). No email, key id, peer id, credential, or timestamp is ever stored,
and attestations are verified statelessly, so no device key id is kept either.

## Admission

Relays accept only credential holders, without learning anything else. A client
presents a zero-knowledge proof of its credential that reveals only the epoch,
bound to the relay's peer id, a fresh nonce, and a timestamp:

- **libp2p**: one `/zkrp/admit/1` request right after connecting. Peers not
  admitted within 10 seconds are disconnected.
- **gRPC**: `BlobStore`, `Logging` and `MailboxService` calls carry the proof in
  `x-zkrp-admit-bin`. `RelayService` stays open for discovery.

Proofs are freshly randomized, so a relay cannot link two connections to the
same credential, or a connection to an enrollment. Relays verify offline against
the published issuer public key, which also stops the issuer from tagging users
with per-user keys. Abuse is bounded per connection and by epoch length, never
by identity.

## Encrypted P2P substrate

Nodes communicate over libp2p (gossipsub for pub/sub, Kademlia for discovery,
circuit relay for reachability). `libghost` wraps this, and messages ride inside
a `GhostEnvelope` (version, time nonce, opaque payload). Payloads are encrypted
end to end; relays route without reading them.

## Account and transport identity

Each device has two keys. The **account** key is long-term: it is the identity
applications see (profiles, DM topics, end-to-end encryption), and it signs
every mesh message together with its topic and a timestamp. The **transport**
key is the libp2p identity, generated fresh on every launch.

A signed message names its account, so anything published in the clear tells
every relay that forwards it which account is behind the connection. Apps
therefore publish on **sealed topics** (`libghost::sealed`): the whole signed
message (account, signature, content type, payload) is encrypted under a key
registered for the topic and travels inside an anonymous outer message. Relays
see transport peer ids, IP addresses, opaque topic names and ciphertext, so they
can't link one session to the next or a connection to an account. A sealed topic
accepts only sealed messages, and in sealed-only mode a context publishes
nothing in the clear: the hello that tells newly connected peers which account
is behind a connection goes only to the app's chosen sealed topics. Message
lengths are not hidden.

Topic keys come from the app: a community key (see enrollment) for
community-wide topics and per-account inboxes derived from it, or a key two
people share for their conversation. Receivers open the message, then
attribute it to the account that signed it, not to the gossipsub neighbour that
forwarded it, and drop unsigned, forged, or stale (more than 10 minutes off)
messages.

IP addresses remain visible to relays, and a stable home IP links sessions on
its own. Hiding that needs a mixnet or Tor in front of the relays.

## Control plane

The `controller` runs a relay node and a gRPC control plane (`RelayService`,
`BlobStore`, `Logging`, `MailboxService`). This is how clients discover relays,
observe health and stats, move blobs, and use store-and-forward mailboxes.

Relays keep a stable identity on disk, so their `/p2p/` addresses survive
restarts. With `DATABASE_URL` set, each relay heartbeats into a shared Postgres
registry and `ListRelays` returns every live relay. The registry is served from
memory, so a database outage degrades discovery to "this relay only" instead of
failing it.

## Store-and-forward

Because gossipsub is ephemeral, a message for an offline recipient is stored as
an opaque, end-to-end-encrypted blob under a rotating, capability-style
`mailbox_id` and fetched on reconnect. The relay stores only ciphertext keyed by
mailbox id, in S3 under a lifecycle rule, with no link to identity.

## Push

A device registers its push token under each mailbox it watches, **sealed**
to the push service's public key (`zkcred::sealed_box`, a fresh ephemeral key
every time) and filed under a tag derived from a device secret and the mailbox
id. Relays store the boxes beside the mailbox (`push/` in S3, same lifecycle)
and can neither open them nor link them to each other or across mailboxes.
When something is deposited, the relay hands that mailbox's boxes, minus the
sender's own tag, to `zkrp-push` on the private network; it opens them and
sends a content-free alert. The push service sees device tokens but never a
mailbox id or message; relays see mailbox ids but never a token. Someone who
runs and logs both could correlate them by timing, so neither logs either,
and Apple sees only that a device got an alert.
