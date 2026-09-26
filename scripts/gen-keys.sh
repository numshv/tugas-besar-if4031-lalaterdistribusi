#!/bin/sh
set -eu

DIR="$(dirname "$0")/../infra/keys"
mkdir -p "$DIR"

if [ -f "$DIR/jwt_private.pem" ]; then
  echo "key pair already exists in $DIR; remove it first to regenerate"
  exit 0
fi

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$DIR/jwt_private.pem"
openssl rsa -in "$DIR/jwt_private.pem" -pubout -out "$DIR/jwt_public.pem"
chmod 600 "$DIR/jwt_private.pem"

echo "key pair created in $DIR"
