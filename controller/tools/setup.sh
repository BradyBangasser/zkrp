#!/bin/bash
set -euo pipefail

REPO_URL="${1:?usage: setup.sh <repo-url> <aws-key-id> <aws-secret>}"
AWS_KEY_ID="${2:?}"
AWS_SECRET="${3:?}"

useradd -r -m -d /opt/zkrp -s /bin/false zkrp 2>/dev/null || true
ZKRP_UID=$(id -u zkrp)
loginctl enable-linger zkrp

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

echo "net.ipv4.ip_unprivileged_port_start=80" >/etc/sysctl.d/99-zkrp.conf
sysctl -p /etc/sysctl.d/99-zkrp.conf

install -m 644 /opt/zkrp/repo/controller/relay.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now relay

history -c
history -w

echo "Done. Check: journalctl -fu zrp-relay"
