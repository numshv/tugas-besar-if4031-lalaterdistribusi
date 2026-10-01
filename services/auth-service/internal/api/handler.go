package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/auth-service/internal/auth"
)

type Handler struct {
	issuer *auth.Issuer
	log    *slog.Logger
}

func NewHandler(issuer *auth.Issuer, log *slog.Logger) *Handler {
	return &Handler{issuer: issuer, log: log}
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_body", "malformed request body")
		return
	}

	ident, ok := auth.Verify(req.Username, req.Password)
	if !ok {
		httpx.Error(w, http.StatusUnauthorized, "invalid_credentials", "invalid credentials")
		return
	}

	access, refresh, err := h.issuer.IssuePair(ident.Username, ident.Scope)
	if err != nil {
		h.log.ErrorContext(r.Context(), "failed to issue tokens", "error", err)
		httpx.Error(w, http.StatusInternalServerError, "token_error", "failed to issue tokens")
		return
	}

	httpx.JSON(w, http.StatusOK, map[string]string{
		"access_token":  access,
		"refresh_token": refresh,
	})
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func (h *Handler) Refresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_body", "malformed request body")
		return
	}

	newAccess, newRefresh, err := h.issuer.Refresh(req.RefreshToken)
	if err != nil {
		httpx.Error(w, http.StatusUnauthorized, "invalid_refresh_token", "refresh token is invalid, expired, or already used")
		return
	}

	httpx.JSON(w, http.StatusOK, map[string]string{
		"access_token":  newAccess,
		"refresh_token": newRefresh,
	})
}

func (h *Handler) JWKS(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, http.StatusOK, h.issuer.JWKS())
}
