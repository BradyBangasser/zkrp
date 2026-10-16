# syntax=docker/dockerfile:1.7
# zkrp's images, from one Dockerfile. The `build` stage compiles both Rust
# binaries in a single `cargo build`; each image is a final stage that copies
# its own binary out of it, so building all of them (docker-bake.hcl, CI)
# compiles the workspace once per platform. Build from the repository root:
#
#   docker buildx bake -f docker-bake.hcl     # all three: relay, auth, caddy
#   docker build --target relay -t zkrp-relay .
#   docker build --target auth  -t zkrp-auth .
#   docker build --target caddy -t zkrp-caddy .

# ── The compile ────────────────────────────────────────────────────────────
FROM docker.io/library/rust:1.96-slim-trixie AS build
WORKDIR /src
# Only the workspace, so editing deploy files doesn't rebuild Rust. protoc
# is vendored by the build scripts; no system protobuf needed.
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY proto proto
# Per-platform caches: the amd64 and arm64 builds run side by side and must
# not share one target directory.
ARG TARGETPLATFORM
RUN --mount=type=cache,id=cargo-registry-${TARGETPLATFORM},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-target-${TARGETPLATFORM},target=/src/target,sharing=locked \
    cargo build --release --locked -p zkrp-relay -p zkrp-auth \
 && install -D -t /out target/release/zkrp-relay target/release/zkrp-auth

# ── The common runtime ─────────────────────────────────────────────────────
FROM docker.io/library/debian:trixie-slim AS runtime
# ca-certificates: TLS to S3, SES, KMS, and the public-IP lookup.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ── relay: libp2p circuit relay + gRPC control plane ──────────────────────
# Run with deploy/relay/compose.yml (deploy/README.md).
FROM runtime AS relay
RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin zkrp \
 && mkdir -p /data && chown zkrp: /data
COPY --from=build /out/zkrp-relay /usr/local/bin/zkrp-relay
# The commit this image was built from (CI passes it); Health reports it.
ARG GIT_SHA=""
ENV ZKRP_BUILD_SHA=${GIT_SHA}
USER zkrp
# The relay key must live on the volume: a new key means a new peer id, which
# breaks every client that cached this relay's address.
VOLUME /data
ENV PORT=9000 GRPC_PORT=9001 IDENTITY_PATH=/data/relay_identity.key
EXPOSE 9000/tcp 9000/udp 9001/tcp 9464/tcp 9466/tcp
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD ["/usr/local/bin/zkrp-relay", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/zkrp-relay"]

# ── auth: enrollment and credential issuance ───────────────────────────────
# Run with compose.yml. Also generates the deployment's keys into its
# secrets volume (deploy/README.md):
#
#   docker compose run --rm --no-deps auth keygen /secrets
FROM runtime AS auth
# /secrets is owned by the service user so a named volume mounted there
# inherits that ownership and `keygen` can write into it. Not /run/secrets:
# Docker's secrets and podman's mounts.conf (Fedora/RHEL) claim that path.
RUN useradd --system --uid 10002 --no-create-home --shell /usr/sbin/nologin zkrp-auth \
 && mkdir -p /secrets && chown zkrp-auth: /secrets && chmod 700 /secrets
COPY --from=build /out/zkrp-auth /usr/local/bin/zkrp-auth
ARG GIT_SHA=""
ENV ZKRP_BUILD_SHA=${GIT_SHA}
USER zkrp-auth
# Secrets are files here, never environment variables.
ENV AUTH_PORT=9002 CREDENTIALS_DIRECTORY=/secrets
EXPOSE 9002/tcp 9465/tcp
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD ["/usr/local/bin/zkrp-auth", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/zkrp-auth"]

# ── caddy: TLS in front of the gRPC APIs ───────────────────────────────────
# Stock Caddy plus the Cloudflare DNS plugin (DNS-01 certificates for the
# shared bootstrap name), with zkrp's configs baked in so hosts need no repo
# checkout. CADDY_ROLE picks /etc/caddy/<role>.Caddyfile: core, relay,
# relay-bootstrap.
FROM docker.io/library/caddy:2.10-builder AS caddy-build
RUN xcaddy build --with github.com/caddy-dns/cloudflare

FROM docker.io/library/caddy:2.10 AS caddy
COPY --from=caddy-build /usr/bin/caddy /usr/bin/caddy
COPY deploy/caddy/ /etc/caddy/
ENV CADDY_ROLE=relay
CMD ["sh", "-c", "exec caddy run --config \"/etc/caddy/$CADDY_ROLE.Caddyfile\" --adapter caddyfile"]
