package auth

import (
	"crypto/subtle"
	"fmt"
	"os"
)

type Identity struct {
	Username string
	Scope    string
}

type credential struct {
	password string
	scope    string
}


var store map[string]credential

func Load() error {
	store = make(map[string]credential)
	configured := make(map[string]credential, 3)
	clients := []struct {
		idEnv     string
		secretEnv string
		scope     string
	}{
		{"CLIENT_MEDIA_ID", "CLIENT_MEDIA_SECRET", "media"},
		{"CLIENT_FIELD_ID", "CLIENT_FIELD_SECRET", "field"},
		{"CLIENT_OPS_ID", "CLIENT_OPS_SECRET", "internal"},
	}

	for _, client := range clients {
		username := os.Getenv(client.idEnv)
		password := os.Getenv(client.secretEnv)
		if username == "" || password == "" {
			return fmt.Errorf("%s and %s must both be set", client.idEnv, client.secretEnv)
		}
		if _, exists := configured[username]; exists {
			return fmt.Errorf("duplicate client ID configured in %s", client.idEnv)
		}
		configured[username] = credential{password: password, scope: client.scope}
	}

	analyticsID, analyticsSecret := os.Getenv("CLIENT_ANALYTICS_ID"), os.Getenv("CLIENT_ANALYTICS_SECRET")
	switch {
	case analyticsSecret == "":
	case analyticsID == "":
		return fmt.Errorf("CLIENT_ANALYTICS_SECRET is set but CLIENT_ANALYTICS_ID is empty")
	default:
		if _, exists := configured[analyticsID]; exists {
			return fmt.Errorf("duplicate client ID configured in CLIENT_ANALYTICS_ID")
		}
		configured[analyticsID] = credential{password: analyticsSecret, scope: "analytics"}
	}

	store = configured
	return nil
}

func Verify(username, password string) (Identity, bool) {
	entry, ok := store[username]
	if !ok {
		subtle.ConstantTimeCompare([]byte(password), []byte("dummy"))
		return Identity{}, false
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(entry.password)) != 1 {
		return Identity{}, false
	}
	return Identity{Username: username, Scope: entry.scope}, true
}
