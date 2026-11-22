package jwtauth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	ScopeMedia     = "media"
	ScopeField     = "field"
	ScopeInternal  = "internal"
	ScopeAnalytics = "analytics"
)

const ClockLeeway = 30 * time.Second

var ErrInvalidToken = errors.New("token is invalid or expired")

type KeyProvider struct {
	mu        sync.RWMutex
	publicKey *rsa.PublicKey
	jwksURL   string
	client    *http.Client
}

func NewKeyProvider(ctx context.Context, jwksURL string) (*KeyProvider, error) {
	kp := &KeyProvider{jwksURL: jwksURL, client: &http.Client{Timeout: 5 * time.Second}}
	if err := kp.refresh(ctx); err != nil {
		return nil, fmt.Errorf("initial JWKS fetch failed: %w", err)
	}
	return kp, nil
}

// NewBackgroundKeyProvider returns at once and fetches the key in the background,
// retrying every interval. Until the first fetch succeeds every token is rejected,
// which keeps a service usable for its other endpoints while auth-service starts.
func NewBackgroundKeyProvider(ctx context.Context, jwksURL string, log *slog.Logger, interval time.Duration) *KeyProvider {
	kp := &KeyProvider{jwksURL: jwksURL, client: &http.Client{Timeout: 5 * time.Second}}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if err := kp.refresh(ctx); err != nil {
				log.Warn("jwks fetch failed, will retry", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return kp
}

func NewStaticKeyProvider(pub *rsa.PublicKey) *KeyProvider {
	return &KeyProvider{publicKey: pub}
}

func (kp *KeyProvider) Get() *rsa.PublicKey {
	kp.mu.RLock()
	defer kp.mu.RUnlock()
	return kp.publicKey
}

func (kp *KeyProvider) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, kp.jwksURL, nil)
	if err != nil {
		return err
	}
	resp, err := kp.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var set struct {
		Keys []struct {
			N string `json:"n"`
			E string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return err
	}
	if len(set.Keys) == 0 {
		return errors.New("JWKS response has no keys")
	}
	n, err := base64.RawURLEncoding.DecodeString(set.Keys[0].N)
	if err != nil {
		return fmt.Errorf("decode modulus: %w", err)
	}
	e, err := base64.RawURLEncoding.DecodeString(set.Keys[0].E)
	if err != nil {
		return fmt.Errorf("decode exponent: %w", err)
	}
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}

	kp.mu.Lock()
	kp.publicKey = pub
	kp.mu.Unlock()
	return nil
}

// StartAutoRefresh re-fetches the key in the background so key rotation needs no redeploy.
func (kp *KeyProvider) StartAutoRefresh(ctx context.Context, log *slog.Logger, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := kp.refresh(ctx); err != nil {
					log.Warn("jwks refresh failed, keeping previous key", "error", err)
				}
			}
		}
	}()
}

type Verifier struct {
	Keys     *KeyProvider
	Issuer   string
	Audience string
}

func (v Verifier) Verify(token string) (scope, subject string, err error) {
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		pub := v.Keys.Get()
		if pub == nil {
			return nil, errors.New("public key not yet available")
		}
		return pub, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
		jwt.WithIssuer(v.Issuer), jwt.WithAudience(v.Audience), jwt.WithExpirationRequired(), jwt.WithLeeway(ClockLeeway))
	if err != nil || !parsed.Valid {
		return "", "", ErrInvalidToken
	}
	scope, _ = claims["scope"].(string)
	subject, _ = claims["sub"].(string)
	if claims["type"] != "access" || subject == "" {
		return "", "", ErrInvalidToken
	}
	switch scope {
	case ScopeMedia, ScopeField, ScopeInternal, ScopeAnalytics:
		return scope, subject, nil
	}
	return "", "", ErrInvalidToken
}

func BearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	return parts[1], true
}
