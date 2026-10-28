#!/usr/bin/env bash
# Run the whole zkrp system locally (deploy/local/compose.yml).
#
#   scripts/local-up.sh          build and start everything on 127.0.0.1
#   scripts/local-up.sh --lan    also reachable from a phone on this network
#                                (plaintext: for development only)
#
# The first run makes the deployment's keys in auth's secrets volume and
# writes ISSUER_PUBLIC_KEY to deploy/local/.env (git-ignored). It also copies
# the issuer and community keys to deploy/local/secrets/, so the FratRat test
# CLI can give itself a credential:  DEV_ISSUER_KEY=deploy/local/secrets/issuer-key
# (it reads community-key from the same directory).
#
# Stop with:   docker compose -f deploy/local/compose.yml down
# Start over:  docker compose -f deploy/local/compose.yml down -v && rm -r deploy/local/.env deploy/local/secrets
# With podman: DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock scripts/local-up.sh
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f deploy/local/compose.yml)
env_file=deploy/local/.env
secrets=deploy/local/secrets

set_env() {
  local key=$1 value=$2
  touch "$env_file"
  if grep -q "^$key=" "$env_file"; then
    sed -i "s|^$key=.*|$key=$value|" "$env_file"
  else
    echo "$key=$value" >> "$env_file"
  fi
}

lan=false
[[ "${1:-}" == "--lan" ]] && lan=true
if $lan; then
  ip=$(ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p')
  [[ -n "$ip" ]] || { echo "could not find this machine's LAN address" >&2; exit 1; }
  set_env LOCAL_IP "$ip"
  set_env LOCAL_BIND 0.0.0.0
else
  set_env LOCAL_IP 127.0.0.1
  set_env LOCAL_BIND 127.0.0.1
fi

echo "== Local keys (auth's secrets volume; only missing ones are made)"
# A placeholder so compose can build and run the key generator the first time.
grep -q '^ISSUER_PUBLIC_KEY=[0-9a-f]' "$env_file" 2>/dev/null || set_env ISSUER_PUBLIC_KEY placeholder
"${compose[@]}" build auth
run_auth() { "${compose[@]}" run --rm --no-deps "$@"; }
key=$(run_auth auth keygen /secrets | sed -n 's/^ISSUER_PUBLIC_KEY=//p' | tr -d '\r')
[[ -n "$key" ]] || { echo "keygen produced no key" >&2; exit 1; }
set_env ISSUER_PUBLIC_KEY "$key"
mkdir -p "$secrets"
for name in issuer-key community-key; do
  run_auth --entrypoint cat auth "/secrets/$name" > "$secrets/$name"
  chmod 600 "$secrets/$name"
done

echo "== Building and starting"
"${compose[@]}" up -d --build

addr=$(sed -n 's/^LOCAL_IP=//p' "$env_file")
cat <<EOF

zkrp is running:
  relay gRPC   http://$addr:9001    (app scheme: GRPC_ADDR=http://$addr:9001)
  relay libp2p /ip4/$addr/tcp/9000
  auth gRPC    http://$addr:9002    (magic links: ${compose[*]} logs -f auth)
  S3           http://localhost:8333
  Prometheus   http://localhost:9090

Test CLI with a credential:
  DEV_ISSUER_KEY=$PWD/$secrets/issuer-key cargo run -p ghost-cli   # in FratRat/rust
EOF
