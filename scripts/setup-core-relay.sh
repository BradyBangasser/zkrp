#!/usr/bin/env bash
# Write deploy/env/caddy.env and deploy/env/relay.env for a relay on the core
# host (compose.relay.yml), asking for each setting. Run it on the core host
# from the checkout, after the core itself is set up (postgres.env, auth.env,
# keygen). It never touches postgres.env or auth.env.
#
#   scripts/setup-core-relay.sh           ask, then write the two files
#   scripts/setup-core-relay.sh --start   and then start the core and the relay
#
# Defaults come from the files already there, then from postgres.env,
# auth.env and the running auth (ISSUER_PUBLIC_KEY), then the templates. Press
# Enter to keep one. Existing files are kept as <file>.bak.
set -euo pipefail
cd "$(dirname "$0")/.."

start=false
[ "${1:-}" = --start ] && start=true

env_dir=deploy/env
caddy_env=$env_dir/caddy.env
relay_env=$env_dir/relay.env

for f in postgres auth; do
  [ -f "$env_dir/$f.env" ] || { echo "$env_dir/$f.env is missing: set up the core first (deploy/README.md)" >&2; exit 1; }
done

# The value of KEY in FILE (uncommented), or nothing.
get() {
  [ -f "$2" ] || return 0
  sed -n "s/^$1=//p" "$2" | tail -n 1
}

# The value of KEY in FILE, else in its template.
get_or_template() {
  local v; v=$(get "$1" "$2")
  printf '%s' "${v:-$(get "$1" "$2.example")}"
}

# ask VAR "question" default [secret]: sets VAR; a secret isn't echoed, and
# its default is only shown as kept.
ask() {
  local var=$1 question=$2 default=$3 secret=${4:-} answer shown
  if [ -n "$secret" ]; then
    shown=${default:+(Enter keeps the current one)}
    read -rsp "$question ${shown:+$shown }: " answer; echo
  else
    read -rp "$question${default:+ [$default]}: " answer
  fi
  printf -v "$var" '%s' "${answer:-$default}"
}

# Percent-encode for a URL's password part.
urlencode() {
  local s=$1 out= c i
  for ((i = 0; i < ${#s}; i++)); do
    c=${s:i:1}
    case "$c" in [a-zA-Z0-9._~-]) out+=$c ;; *) printf -v c '%%%02X' "'$c"; out+=$c ;; esac
  done
  printf '%s' "$out"
}

# write FILE TEMPLATE KEY...: FILE from TEMPLATE (comments kept), with each
# KEY set to the shell variable of that name, uncommenting it if the
# template has it commented out, appending it otherwise.
write() {
  local file=$1 template=$2; shift 2
  local tmp; tmp=$(mktemp "$file.XXXXXX")
  local key
  for key in "$@"; do export "SET_$key=${!key}"; done
  awk -v keys="$*" '
    BEGIN { n = split(keys, k, " "); for (i = 1; i <= n; i++) want[k[i]] = 1 }
    {
      line = $0; sub(/^#/, "", line)
      split(line, kv, "=")
      key = kv[1]
      if (index(line, "=") && (key in want) && !(key in done)) {
        print key "=" ENVIRON["SET_" key]; done[key] = 1; next
      }
      print
    }
    END { for (i = 1; i <= n; i++) if (!(k[i] in done)) print k[i] "=" ENVIRON["SET_" k[i]] }
  ' "$template" >"$tmp"
  chmod 600 "$tmp"
  [ -f "$file" ] && cp -p "$file" "$file.bak"
  mv "$tmp" "$file"
  echo "wrote $file"
}

# Settings already known elsewhere on this host.
epoch_secs=$(get EPOCH_SECS "$env_dir/auth.env")
relay_db_password=$(get RELAY_DB_PASSWORD "$env_dir/postgres.env")
[ -n "$relay_db_password" ] || { echo "RELAY_DB_PASSWORD is empty in $env_dir/postgres.env" >&2; exit 1; }
issuer_key=$(get ISSUER_PUBLIC_KEY "$relay_env")
if [ -z "$issuer_key" ]; then
  issuer_key=$(docker compose logs auth 2>/dev/null |
    sed -n 's/.*issuer_public_key=\([0-9a-f]*\).*/\1/p' | tail -n 1 || true)
fi

echo "== Caddy (in front of auth and the relay)"
ask AUTH_DOMAIN "Auth domain" "$(get AUTH_DOMAIN "$caddy_env")"
ask RELAY_DOMAIN "This relay's domain (e.g. relay.a.use2.us.infra.zkrp.net)" \
  "$(get RELAY_DOMAIN "$caddy_env")"
role_default=$(get CADDY_ROLE "$caddy_env")
case "$role_default" in core-relay | core-relay-bootstrap) ;; *) role_default=core-relay-bootstrap ;; esac
ask bootstrap "Also answer for the shared bootstrap name? (y/n)" \
  "$([ "$role_default" = core-relay-bootstrap ] && echo y || echo n)"
case "$bootstrap" in
  [yY]*)
    CADDY_ROLE=core-relay-bootstrap
    current=$(get BOOTSTRAP_DOMAIN "$caddy_env")
    ask BOOTSTRAP_DOMAIN "Bootstrap domain" "${current:-bootstrap.zkrp.net}"
    ask CLOUDFLARE_API_TOKEN "Cloudflare API token (Zone → DNS → Edit)" \
      "$(get CLOUDFLARE_API_TOKEN "$caddy_env")" secret
    [ -n "$CLOUDFLARE_API_TOKEN" ] || { echo "the bootstrap name needs a Cloudflare token" >&2; exit 1; }
    caddy_keys=(CADDY_ROLE AUTH_DOMAIN RELAY_DOMAIN BOOTSTRAP_DOMAIN CLOUDFLARE_API_TOKEN)
    ;;
  *)
    CADDY_ROLE=core-relay
    caddy_keys=(CADDY_ROLE AUTH_DOMAIN RELAY_DOMAIN)
    ;;
esac

echo
echo "== Relay"
current=$(get REGION "$relay_env")
ask REGION "Region" "${current:-us-east-2}"
ask RELAY_MESHES "Meshes (comma-separated)" "$(get_or_template RELAY_MESHES "$relay_env")"
ask ISSUER_PUBLIC_KEY "Issuer public key (from keygen)" "$issuer_key"
[ -n "$ISSUER_PUBLIC_KEY" ] || { echo "the relay needs ISSUER_PUBLIC_KEY" >&2; exit 1; }
ask AWS_REGION "AWS region" "$(get_or_template AWS_REGION "$relay_env")"
ask AWS_ACCESS_KEY_ID "AWS access key id (empty: the instance role)" "$(get AWS_ACCESS_KEY_ID "$relay_env")"
AWS_SECRET_ACCESS_KEY=$(get AWS_SECRET_ACCESS_KEY "$relay_env")
if [ -n "$AWS_ACCESS_KEY_ID" ]; then
  ask AWS_SECRET_ACCESS_KEY "AWS secret access key" "$AWS_SECRET_ACCESS_KEY" secret
else
  AWS_SECRET_ACCESS_KEY=
fi
ask BLOB_BUCKET "S3 bucket for blobs and mailboxes" "$(get BLOB_BUCKET "$relay_env")"
ask LOG_BUCKET "S3 bucket for debug logs" "$(get LOG_BUCKET "$relay_env")"
ask CF_DOMAIN "CF_DOMAIN" "$(get CF_DOMAIN "$relay_env")"

# Derived from the answers and the core's own settings.
RELAY_PUBLIC_ADDRS=/dns4/$RELAY_DOMAIN/tcp/9000
PUBLIC_GRPC_URL=https://$RELAY_DOMAIN
AUTH_URL=https://$AUTH_DOMAIN
EPOCH_SECS=${epoch_secs:-2592000}
DATABASE_URL=postgres://relay:$(urlencode "$relay_db_password")@postgres/registry

echo
write "$caddy_env" "$env_dir/caddy.env.example" "${caddy_keys[@]}"
write "$relay_env" "$env_dir/relay.env.example" \
  RELAY_PUBLIC_ADDRS PUBLIC_GRPC_URL REGION RELAY_MESHES ISSUER_PUBLIC_KEY \
  EPOCH_SECS AUTH_URL DATABASE_URL AWS_REGION AWS_ACCESS_KEY_ID \
  AWS_SECRET_ACCESS_KEY BLOB_BUCKET LOG_BUCKET CF_DOMAIN

cat <<EOF

Before starting: DNS (grey cloud) for $RELAY_DOMAIN${BOOTSTRAP_DOMAIN:+ and $BOOTSTRAP_DOMAIN}
at this host, and 9000 TCP+UDP open in the firewall.
EOF

if $start; then
  docker compose pull && docker compose up -d
  docker compose -f compose.relay.yml pull && docker compose -f compose.relay.yml up -d
  echo "check: docker compose -f compose.relay.yml logs -f relay"
else
  cat <<EOF
Then:
  docker compose pull && docker compose up -d
  docker compose -f compose.relay.yml pull && docker compose -f compose.relay.yml up -d
EOF
fi
