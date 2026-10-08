#!/bin/sh
set -eu

DIR="$(dirname "$0")/../infra/keys"
PRIV="$DIR/jwt_private.pem"
PUB="$DIR/jwt_public.pem"
mkdir -p "$DIR"

for f in "$PRIV" "$PUB"; do
  if [ -d "$f" ]; then
    rmdir "$f"
    echo "removed empty directory left by docker at $f"
  fi
done

if [ -f "$PRIV" ] && [ -f "$PUB" ]; then
  echo "key pair already exists in $DIR; remove it first to regenerate"
  exit 0
fi

if [ ! -f "$PRIV" ]; then
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$PRIV"
  chmod 600 "$PRIV"
fi
openssl rsa -in "$PRIV" -pubout -out "$PUB"

echo "key pair ready in $DIR"
