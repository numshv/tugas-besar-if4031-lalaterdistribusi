#!/usr/bin/env bash
# Run on node-b, node-c and node-d (workers).
# Usage: sudo NODE_NAME=node-b SERVER_IP=... TOKEN=... [NODE_IP=...] [FLANNEL_IFACE=...] bash install-agent.sh
set -euo pipefail

: "${NODE_NAME:?set NODE_NAME, e.g. node-b}" "${SERVER_IP:?set SERVER_IP of node-a}" "${TOKEN:?set TOKEN from install-server.sh}"
args=(agent --node-name "$NODE_NAME")
[ -n "${NODE_IP:-}" ] && args+=(--node-ip "$NODE_IP")
[ -n "${FLANNEL_IFACE:-}" ] && args+=(--flannel-iface "$FLANNEL_IFACE")

curl -sfL https://get.k3s.io | K3S_URL="https://$SERVER_IP:6443" K3S_TOKEN="$TOKEN" INSTALL_K3S_EXEC="${args[*]}" sh -
