package auth

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const keyID = "bnpb-auth-1"

type Issuer struct {
	privateKey *rsa.PrivateKey
	accessTTL  time.Duration
	refreshTTL time.Duration
	issuer     string
	audience   string
	statePath  string
	mu         sync.Mutex
	revokedJTI map[string]time.Time
}

func NewIssuer(privateKey *rsa.PrivateKey, accessTTL, refreshTTL time.Duration, issuer, audience, statePath string) (*Issuer, error) {
	if privateKey == nil || accessTTL <= 0 || refreshTTL <= 0 || issuer == "" || audience == "" || statePath == "" {
		return nil, errors.New("invalid token issuer configuration")
	}
	if err := os.MkdirAll(filepath.Dir(statePath), 0700); err != nil {
		return nil, err
	}
	revokedJTI, err := loadRevocations(statePath)
	if err != nil {
		return nil, err
	}
	return &Issuer{
		privateKey: privateKey,
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
		issuer:     issuer,
		audience:   audience,
		statePath:  statePath,
		revokedJTI: revokedJTI,
	}, nil
}

func (iss *Issuer) IssuePair(username, scope string) (access, refresh string, err error) {
	if username == "" || !validScope(scope) {
		return "", "", jwt.ErrTokenInvalidClaims
	}
	access, err = iss.sign(username, scope, "access", iss.accessTTL)
	if err != nil {
		return "", "", err
	}
	refresh, err = iss.sign(username, scope, "refresh", iss.refreshTTL)
	if err != nil {
		return "", "", err
	}
	return access, refresh, nil
}

func (iss *Issuer) sign(username, scope, tokenType string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":   username,
		"scope": scope,
		"type":  tokenType, 
		"iss":   iss.issuer,
		"aud":   iss.audience,
		"iat":   now.Unix(),
		"exp":   now.Add(ttl).Unix(),
		"jti":   uuid.NewString(),
	}
	t := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	t.Header["kid"] = keyID
	return t.SignedString(iss.privateKey)
}

func (iss *Issuer) Refresh(refreshToken string) (newAccess, newRefresh string, err error) {
	claims, err := iss.parseAndVerify(refreshToken)
	if err != nil {
		return "", "", err
	}
	if claims["type"] != "refresh" {
		return "", "", jwt.ErrTokenInvalidClaims
	}

	jti, _ := claims["jti"].(string)
	username, _ := claims["sub"].(string)
	scope, _ := claims["scope"].(string)
	expiration, err := claims.GetExpirationTime()
	if err != nil || jti == "" || username == "" || !validScope(scope) || expiration == nil {
		return "", "", jwt.ErrTokenInvalidClaims
	}
	newAccess, newRefresh, err = iss.IssuePair(username, scope)
	if err != nil {
		return "", "", err
	}

	iss.mu.Lock()
	defer iss.mu.Unlock()

	now := time.Now()
	nextRevocations := make(map[string]time.Time, len(iss.revokedJTI)+1)
	for revokedJTI, expiresAt := range iss.revokedJTI {
		if expiresAt.After(now) {
			nextRevocations[revokedJTI] = expiresAt
		}
	}
	if _, revoked := nextRevocations[jti]; revoked {
		return "", "", jwt.ErrTokenInvalidClaims
	}
	nextRevocations[jti] = expiration.Time
	if err := persistRevocations(iss.statePath, nextRevocations); err != nil {
		return "", "", err
	}
	iss.revokedJTI = nextRevocations
	return newAccess, newRefresh, nil
}

func (iss *Issuer) parseAndVerify(tokenStr string) (jwt.MapClaims, error) {
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodRS256 {
			return nil, errors.New("unexpected signing method")
		}
		return &iss.privateKey.PublicKey, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}), jwt.WithIssuer(iss.issuer), jwt.WithAudience(iss.audience), jwt.WithExpirationRequired())
	if err != nil {
		return nil, err
	}
	if !parsed.Valid {
		return nil, jwt.ErrTokenInvalidClaims
	}
	return claims, nil
}

func (iss *Issuer) JWKS() map[string]any {
	publicKey := &iss.privateKey.PublicKey
	exponent := big.NewInt(int64(publicKey.E)).Bytes()
	return map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": jwt.SigningMethodRS256.Alg(),
			"kid": keyID,
			"n":   base64.RawURLEncoding.EncodeToString(publicKey.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(exponent),
		}},
	}
}

func validScope(scope string) bool {
	switch scope {
	case "media", "field", "internal":
		return true
	default:
		return false
	}
}

func loadRevocations(path string) (map[string]time.Time, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[string]time.Time), nil
	}
	if err != nil {
		return nil, err
	}
	var revocations map[string]time.Time
	if err := json.Unmarshal(data, &revocations); err != nil {
		return nil, err
	}
	if revocations == nil {
		revocations = make(map[string]time.Time)
	}
	return revocations, nil
}

func persistRevocations(path string, revocations map[string]time.Time) error {
	data, err := json.Marshal(revocations)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".refresh-revocations-*")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}
