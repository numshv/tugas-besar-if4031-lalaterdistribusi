set -eu

DIR="$(dirname "$0")/../infra/keys"
PRIV="$DIR/jwt_private.pem"
mkdir -p "$DIR"

if [ -d "$PRIV" ]; then
  rmdir "$PRIV"
  echo "removed empty directory left by docker at $PRIV"
fi

if [ -f "$PRIV" ]; then
  echo "private key already exists at $PRIV; remove it first to regenerate"
  exit 0
fi

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$PRIV"
chmod 600 "$PRIV"

echo "private key ready at $PRIV"
