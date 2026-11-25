#!/usr/bin/env bash
# Run on node-a (control plane + worker). Optional: NODE_IP and FLANNEL_IFACE when the VM has
# several interfaces, e.g. FLANNEL_IFACE=tailscale0 for nodes on different laptops.
# Usage: sudo NODE_NAME=node-a [NODE_IP=...] [FLANNEL_IFACE=...] bash install-server.sh
set -euo pipefail

args=(server --node-name "${NODE_NAME:-node-a}" --write-kubeconfig-mode 644 --secrets-encryption)
[ -n "${NODE_IP:-}" ] && args+=(--node-ip "$NODE_IP" --advertise-address "$NODE_IP")
[ -n "${FLANNEL_IFACE:-}" ] && args+=(--flannel-iface "$FLANNEL_IFACE")

curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="${args[*]}" sh -
install -m 600 "$(dirname "$0")/traefik-config.yaml" /var/lib/rancher/k3s/server/manifests/traefik-config.yaml
echo "join token for the agents:"
cat /var/lib/rancher/k3s/server/node-token
