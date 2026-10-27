// Package access decides who may call the administrator API. Stage 7 part 3
// replaces AllowAll with Auth token validation and an AuthZ ADMIN check.
package access

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type Authorizer interface {
	// Authorize returns the user the request acts for, or an error wrapping
	// model.ErrAccessDenied.
	Authorize(r *http.Request) (string, error)
}

type allowAll struct{}

// AllowAll lets every request through as "anonymous"; the port is published
// only on 127.0.0.1 until a real Authorizer is wired.
func AllowAll() Authorizer { return allowAll{} }

func (allowAll) Authorize(*http.Request) (string, error) { return "anonymous", nil }

type initiatorKey struct{}

func Initiator(ctx context.Context) string {
	id, _ := ctx.Value(initiatorKey{}).(string)
	return id
}

func Middleware(a Authorizer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, err := a.Authorize(r)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, model.ErrAccessDenied) {
				status = http.StatusForbidden
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": model.ErrAccessDenied.Error(), "error": err.Error()})
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), initiatorKey{}, user)))
	})
}
