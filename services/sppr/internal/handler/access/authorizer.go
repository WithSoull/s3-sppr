package access

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client/identity"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const CookieName = "sppr_access"

var ErrUnauthenticated = errors.New("authentication required")

type authorizer struct {
	auth        identity.AuthClient
	permissions identity.PermissionClient
}

func NewAuthorizer(auth identity.AuthClient, permissions identity.PermissionClient) Authorizer {
	return &authorizer{auth, permissions}
}
func (a *authorizer) Authorize(r *http.Request) (string, error) {
	token := ""
	if raw := r.Header.Get("Authorization"); raw != "" {
		parts := strings.Fields(raw)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			return "", ErrUnauthenticated
		}
		token = parts[1]
	} else if c, err := r.Cookie(CookieName); err == nil {
		token = c.Value
	}
	if token == "" {
		return "", ErrUnauthenticated
	}
	if err := a.auth.Validate(r.Context(), token); err != nil {
		return "", AuthError(err)
	}
	claims, err := ValidatedClaims(token)
	if err != nil {
		return "", err
	}
	allowed, err := a.permissions.Admin(r.Context(), claims.UserID)
	if err != nil {
		return "", model.ErrUpstreamUnavailable
	}
	if !allowed {
		return "", model.ErrAccessDenied
	}
	return claims.UserID, nil
}

// ValidatedClaims decodes claims only AFTER Auth.Validate has succeeded. SPPR
// has no signing key and never uses an unverified payload to grant access.
type Claims struct {
	UserID    string `json:"user_id"`
	TokenType string `json:"token_type"`
	Exp       int64  `json:"exp"`
}

func ValidatedClaims(token string) (Claims, error) {
	var c Claims
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return c, ErrUnauthenticated
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return c, ErrUnauthenticated
	}
	if json.Unmarshal(raw, &c) != nil || c.TokenType != "access" || c.Exp <= time.Now().Unix() {
		return c, ErrUnauthenticated
	}
	if _, err := uuid.Parse(c.UserID); err != nil {
		return c, ErrUnauthenticated
	}
	return c, nil
}
func AuthError(err error) error {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.ResourceExhausted:
		return ErrUnauthenticated
	default:
		return model.ErrUpstreamUnavailable
	}
}
func Status(err error) int {
	switch {
	case errors.Is(err, ErrUnauthenticated):
		return http.StatusUnauthorized
	case errors.Is(err, model.ErrAccessDenied):
		return http.StatusForbidden
	default:
		return http.StatusServiceUnavailable
	}
}
