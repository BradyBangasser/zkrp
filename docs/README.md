# zkRP documentation

zkRP is an anonymous, encrypted peer-to-peer network. Devices enroll anonymously,
talk over a libp2p relay mesh, and reach offline peers through store-and-forward
mailboxes, with a gRPC control plane for discovery, health, and storage.

- **Architecture** - the components and how they fit together.
- **Services** - the service catalog.
- **gRPC API reference** - full per-service reference, generated from the protos.
- **Reliability** - the 99.9% SLO, the 99% SLA, and the Prometheus metrics that
  verify them.
