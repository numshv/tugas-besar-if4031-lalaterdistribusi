package correlation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

const Header = "X-Correlation-ID"

const maxLen = 128

type ctxKey struct{}

func New() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func FromHeader(v string) string {
	if v == "" || len(v) > maxLen {
		return New()
	}
	for i := 0; i < len(v); i++ {
		if !safeByte(v[i]) {
			return New()
		}
	}
	return v
}

func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

func safeByte(c byte) bool {
	switch {
	case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		return true
	default:
		return c == '-' || c == '_'
	}
}
