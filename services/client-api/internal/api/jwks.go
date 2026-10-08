package api

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"sync"
	"time"
)

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwksResponse struct {
	Keys []jwk `json:"keys"`
}

// KeyProvider menyimpan public key yang diambil dari JWKS endpoint milik
// Auth Service, bukan dibaca dari file shared volume. Ini menjaga prinsip
// "data dimiliki satu service, service lain minta lewat API", dan membuat
// Client API otomatis dapat kunci baru kalau Auth Service rotate kunci,
// tanpa perlu redeploy manual.
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

	var set jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return err
	}
	if len(set.Keys) == 0 {
		return fmt.Errorf("JWKS response has no keys")
	}
	key := set.Keys[0] // auth-service saat ini cuma menerbitkan satu kid

	nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
	if err != nil {
		return fmt.Errorf("decode modulus: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
	if err != nil {
		return fmt.Errorf("decode exponent: %w", err)
	}

	pub := &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: int(new(big.Int).SetBytes(eBytes).Int64()),
	}

	kp.mu.Lock()
	kp.publicKey = pub
	kp.mu.Unlock()
	return nil
}

// StartAutoRefresh secara berkala re-fetch JWKS di background.
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
