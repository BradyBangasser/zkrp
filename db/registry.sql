-- Relay registry. Relay infrastructure only: no client data ever lands here.
-- Each relay upserts its own row every 15s; rows older than 45s are ignored.
CREATE TABLE IF NOT EXISTS relay (
    peer_id      TEXT PRIMARY KEY,
    multiaddr    TEXT        NOT NULL,
    region       TEXT        NOT NULL,
    meshes       TEXT[]      NOT NULL,
    load         INTEGER     NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    grpc_url     TEXT        NOT NULL DEFAULT ''
);

-- Registries created before grpc_url existed.
ALTER TABLE relay ADD COLUMN IF NOT EXISTS grpc_url TEXT NOT NULL DEFAULT '';

REVOKE ALL ON relay FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON relay TO relay;
