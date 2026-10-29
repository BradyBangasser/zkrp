#!/usr/bin/env bash
# Validate every compose project (core, relay, local) and check that every
# bind-mounted path exists. Works on a temporary copy, with env files made
# from deploy/env/*.example, so real env files are never read or touched.
# CI runs it on every push.
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE=${COMPOSE:-docker compose}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp -r compose.yml compose.relay.yml Dockerfile db monitoring deploy "$tmp"/
for f in "$tmp"/deploy/env/*.example; do cp "$f" "${f%.example}"; done
cp "$tmp"/deploy/env/relay.env "$tmp"/deploy/env/caddy.env "$tmp"/deploy/relay/

status=0
check() {
  local name=$1 dir=$2; shift 2
  local out
  if ! out=$(cd "$dir" && ISSUER_PUBLIC_KEY=00 PUSH_PUBLIC_KEY=00 $COMPOSE "$@" config 2>&1); then
    echo "FAIL $name"; echo "$out" | head -5; status=1; return
  fi
  # A missing bind source makes the container start with an empty directory
  # where a file should be.
  local missing
  missing=$(awk '/type: bind/{b=1} b && /source:/{print $2; b=0}' <<<"$out" |
    while read -r src; do
      case "$src" in /var/run/*|*/.docker/*|*/podman/*) continue ;; esac
      [ -e "$src" ] || echo "$src"
    done)
  if [ -n "$missing" ]; then
    echo "FAIL $name: missing bind sources:"; echo "$missing"; status=1
  else
    echo "ok   $name: $(cd "$dir" && ISSUER_PUBLIC_KEY=00 PUSH_PUBLIC_KEY=00 $COMPOSE "$@" config --services | tr '\n' ' ')"
  fi
}

check "core (compose.yml)"            "$tmp"
check "core + monitoring"             "$tmp" --profile monitoring
check "relay (deploy/relay)"          "$tmp/deploy/relay"
check "relay on the core host"        "$tmp" -f compose.relay.yml
check "local (deploy/local)"          "$tmp/deploy/local"
exit $status
