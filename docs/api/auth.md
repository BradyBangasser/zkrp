# AuthService

`package zrp.auth.v1`

Anonymous enrollment. Proves an email address is eligible and issues a blind
credential, without the server ever learning who enrolled.

Enrollment is three unary calls with no open stream: the email round trip takes
minutes, and holding a stream would make the server stateful, which this service
deliberately avoids. State that would normally live server-side is carried by the
client in an opaque, MAC'd `verification_token`. The only thing persisted is a
set of 32-byte nullifiers; never an email, key id, peer id, issued credential, or
any mapping between them. A nullifier answers exactly one question: has this
address enrolled?

## Methods

| Method | Type | Request | Response |
| --- | --- | --- | --- |
| `GetChallenge` | unary | `ChallengeRequest` | `ChallengeResponse` |
| `Redeem` | unary | `RedeemRequest` | `RedeemResponse` |

### GetChallenge

Begins enrollment for an email address and returns a challenge to be satisfied.

`ChallengeRequest`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `email` | `string` | 1 | Address to enroll. |

`ChallengeResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `challenge` | `bytes` | 1 | Opaque challenge the client must satisfy. |

### Redeem

Submits the satisfied challenge and a credential request; returns a blind
signature over the credential.

`RedeemRequest`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `verification_token` | `bytes` | 1 | Opaque, MAC'd token carrying enrollment state client-side. |
| `assertion` | `bytes` | 2 | Proof that the challenge was satisfied. |
| `credential_request` | `bytes` | 3 | Blinded credential request. |

`RedeemResponse`

| Field | Type | # | Notes |
| --- | --- | --- | --- |
| `signature` | `bytes` | 1 | Blind signature over the credential. |
| `issuer_public_key` | `bytes` | 2 | Public key to verify the credential against. |
| `params_label` | `string` | 3 | Label of the parameter set used. |
| `epoch` | `string` | 4 | Issuance epoch. |

## Example

```bash
grpcurl -plaintext -d '{"email":"user@example.com"}' \
  auth-host:9000 zrp.auth.v1.AuthService/GetChallenge
```
