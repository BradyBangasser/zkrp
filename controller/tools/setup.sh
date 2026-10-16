#!/bin/bash
set -euo pipefail

REPO_URL="${1:?usage: setup.sh <repo-url> <aws-key-id> <aws-secret>}"
AWS_KEY_ID="${2:?}"
AWS_SECRET="${3:?}"

useradd -r -m -d /opt/zkrp -s /bin/false zkrp 2>/dev/null || true
ZKRP_UID=$(id -u zkrp)
loginctl enable-linger zkrp
usermod --add-subuids 100000-165535 --add-subgids 100000-165535 zkrp

mkdir -p /opt/zkrp/repo /var/lib/zkrp /etc/zkrp/credentials /opt/zkrp/.config/containers /opt/zkrp/.local/share/containers
mkdir -p /run/user/$ZKRP_UID
chmod 700 /run/user/$ZKRP_UID
chown -R zkrp:zkrp /opt/zkrp /var/lib/zkrp
chown -R zkrp:zkrp /run/user/$ZKRP_UID

sudo -u zkrp git clone "$REPO_URL" /opt/zkrp/repo

printf '%s' "$AWS_KEY_ID" | systemd-creds encrypt --name=aws-key-id - /etc/zkrp/credentials/aws-key-id.cred
printf '%s' "$AWS_SECRET" | systemd-creds encrypt --name=aws-secret - /etc/zkrp/credentials/aws-secret.cred
chmod 600 /etc/zkrp/credentials/*.cred
chown root:root /etc/zkrp/credentials/*.cred

if [ ! -f /etc/system.env ]; then
    cat > /etc/system.env <<'ENV'
IMAGE=ghcr.io/bradybangasser/zkrp-relay
IMAGE_TAG=latest
# Public name of this host; caddy.service serves TLS for it on 443, and
# apps learn https://$RELAY_DOMAIN from ListRelays.
RELAY_DOMAIN=
# Enrollment service URL handed to apps by GetNetworkConfig.
AUTH_URL=
# TLS proxy. Use ghcr.io/bradybangasser/zkrp-caddy (Cloudflare DNS plugin)
# when this relay also serves BOOTSTRAP_DOMAIN (see docs/deployment.md).
CADDY_IMAGE=docker.io/library/caddy:2.10
BOOTSTRAP_DOMAIN=
# Set to the address clients dial, e.g. /dns4/relay.example.net/tcp/9000
RELAY_PUBLIC_ADDRS=
REGION=local
RELAY_MESHES=zkrp
# From `zkrp-auth keygen` on the auth host. Required.
ISSUER_PUBLIC_KEY=
AWS_REGION=us-east-2
BLOB_BUCKET=
LOG_BUCKET=
CF_DOMAIN=
ENV
    chmod 644 /etc/system.env
    echo "Edit /etc/system.env (RELAY_PUBLIC_ADDRS especially) before starting."
fi

install -m 644 /opt/zkrp/repo/controller/relay.service /etc/systemd/system/
install -m 644 /opt/zkrp/repo/controller/caddy.service /etc/systemd/system/
mkdir -p /var/lib/zkrp-caddy /etc/zkrp/caddy.d
systemctl daemon-reload
systemctl enable --now relay caddy

history -c
history -w

echo "Done. Check: journalctl -fu relay"
