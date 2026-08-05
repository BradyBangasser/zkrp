---
title: "zkRP: Encrypted P2P with Zero-Knowledge Range Proofs"
summary: "An encrypted peer-to-peer network of APIs and services built on zero-knowledge range proofs, operated to a 99.9% SLO."
tags: [cryptography, p2p, reliability, sre, distributed-systems]
featured: true
slo: "99.9%"
sla: "99%"
---

# zkRP

zkRP is an encrypted peer-to-peer network built around zero-knowledge range
proofs. The range-proof primitive lets a node prove a value falls within a
bound without revealing the value itself, and zkRP uses that as the trust layer
for a set of APIs and services that communicate over an encrypted P2P transport.

## What it provides

- **Zero-knowledge range proofs** as the core primitive.
- **An encrypted P2P transport** that services use to communicate.
- **A set of APIs and services** built on top of that transport.

## Reliability

Every service targets a **99.9% SLO**, and customers are offered a **99% SLA**.
Service health and request outcomes are exported to Prometheus, so the SLO is
measured and verifiable rather than asserted. See the reliability doc for the
metrics and the error-budget model.

## Related

[FratRat](https://github.com/BradyBangasser/FratRat) is built on zkRP's
encrypted peer-to-peer network.

Full documentation is under `docs/`: architecture, the service catalog, the
**gRPC API reference** (generated from the protos), and the reliability model.
