#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)/infra/keys"
KEYFILE="$DIR/mongo-keyfile"
mkdir -p "$DIR"

if [ -f "$KEYFILE" ]; then
  echo "keyfile already exists at $KEYFILE; remove it first to regenerate"
  exit 0
fi

openssl rand -base64 756 >"$KEYFILE"
chmod 600 "$KEYFILE"

echo "keyfile ready at $KEYFILE"
