# AuthService

`package zrp.auth.v1`

Anonymous enrollment. Proves an email address is eligible and issues a blind
credential, without the server ever learning who enrolled.

Enrollment is three unary calls with no open stream: the email round trip takes
minutes, and holding a stream would make the server stateful, which this service
deliberately avoids. State that would normally live server-side is carried by the
client in an encrypted (XChaCha20-Poly1305) `verification_token`. The only thing persisted is a
set of 32-byte nullifiers; never an email, key id, peer id, issued credential, or
any mapping between them. A nullifier answers exactly one question: has this
address enrolled?

## Methods

| Method | Type | Request | Response |
| --- | --- | --- | --- |
| `GetChallenge` | unary | `ChallengeRequest` | `ChallengeResponse` |
| `Redeem` | unary | `RedeemRequest` | `RedeemResponse` |

### GetChallenge

Emails a magic link for an eligible address (`ALLOWED_EMAIL_DOMAINS`; `+tag`
aliases are folded into the base address) and returns a challenge. At most 3
links per address per 15 minutes.

`ChallengeRequest`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `email` | `string` | 1 | Address to enroll. |

`ChallengeResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `challenge` | `bytes` | 1 | 32 random bytes. The app attests over `SHA-256("zkrp/attest/v1" \|\| challenge \|\| SHA-256(credential_request))`. |

### Redeem

Redeems the magic link. Verifies the App Attest attestation, claims a nullifier
slot for this address and epoch, and returns a blind signature. Fails with
`ALREADY_EXISTS` once the address has used its 3 slots for the epoch.

`RedeemRequest`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `verification_token` | `bytes` | 1 | From the magic link fragment. Opaque to the client. |
| `attestation` | `bytes` | 2 | A fresh App Attest attestation object (not an assertion). |
| `credential_request` | `bytes` | 3 | `zkcred::request` output: commitment to the link secret plus proof of knowledge. |

`RedeemResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `signature` | `bytes` | 1 | Blind signature over the credential. |
| `issuer_public_key` | `bytes` | 2 | Clients should compare this to a pinned key, not trust it. |
| `params_label` | `string` | 3 | Label of the parameter set used. |
| `epoch` | `uint64` | 4 | `unix_secs / EPOCH_SECS`. |

## Example

```bash
grpcurl -plaintext -d '{"email":"user@example.com"}' \
  auth-host:9002 zrp.auth.v1.AuthService/GetChallenge
```
