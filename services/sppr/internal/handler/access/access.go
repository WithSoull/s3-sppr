// Package access protects the administrator APIs and panel.
package access

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type Authorizer interface {
	// Authorize returns the verified user ID, ErrUnauthenticated,
	// model.ErrAccessDenied or model.ErrUpstreamUnavailable.
	Authorize(r *http.Request) (string, error)
}

type allowAll struct{}

// AllowAll is for explicitly unauthenticated local test stands only.
func AllowAll() Authorizer { return allowAll{} }

func (allowAll) Authorize(*http.Request) (string, error) { return "anonymous", nil }

type initiatorKey struct{}

func Initiator(ctx context.Context) string {
	id, _ := ctx.Value(initiatorKey{}).(string)
	return id
}

func Middleware(a Authorizer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !SameOrigin(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "ACCESS_DENIED", "error": "origin mismatch"})
			return
		}
		user, err := a.Authorize(r)
		if err != nil {
			status := Status(err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": model.ErrAccessDenied.Error(), "error": err.Error()})
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), initiatorKey{}, user)))
	})
}
