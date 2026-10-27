// Package admin serves the Russian administrator panel using embedded templates.
package admin

import (
	"bytes"
	"net/http"
	"strconv"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/google/uuid"
)

func (h *handler) login(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodGet {
		h.render(w, r, 200, "login", view{})
		return
	}
	if !access.SameOrigin(r) {
		h.fail(w, r, 403, "ACCESS_DENIED")
		return
	}
	if err := parseForm(w, r); err != nil {
		h.fail(w, r, 400, "INVALID_ARGUMENT")
		return
	}
	if h.auth == nil {
		h.fail(w, r, 503, "AUTH_UNAVAILABLE")
		return
	}
	token, err := h.auth.Login(r.Context(), r.PostForm.Get("email"), r.PostForm.Get("password"))
	if err != nil {
		code := access.Status(access.AuthError(err))
		key := "ACCESS_DENIED"
		if code == http.StatusServiceUnavailable {
			key = "UPSTREAM_UNAVAILABLE"
		}
		h.fail(w, r, code, key)
		return
	}
	check := r.Clone(r.Context())
	check.Header = r.Header.Clone()
	check.Header.Set("Authorization", "Bearer "+token)
	if _, err = h.authorizer.Authorize(check); err != nil {
		code := access.Status(err)
		key := "ACCESS_DENIED"
		if code == http.StatusServiceUnavailable {
			key = "UPSTREAM_UNAVAILABLE"
		}
		h.fail(w, r, code, key)
		return
	}
	claims, err := access.ValidatedClaims(token)
	if err != nil {
		h.fail(w, r, 401, "ACCESS_DENIED")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: access.CookieName, Value: token, Path: "/", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode, Expires: time.Unix(claims.Exp, 0), MaxAge: int(claims.Exp - time.Now().Unix())})
	http.Redirect(w, r, "/admin", http.StatusSeeOther)
}
func clearCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: access.CookieName, Path: "/", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: -1, Expires: time.Unix(1, 0)})
}
func (h *handler) logout(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !access.SameOrigin(r) {
		h.fail(w, r, 403, "ACCESS_DENIED")
		return
	}
	clearCookie(w, r)
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}
func (h *handler) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if !access.SameOrigin(r) {
			h.fail(w, r, 403, "ACCESS_DENIED")
			return
		}
		user, err := h.authorizer.Authorize(r)
		if err != nil {
			if access.Status(err) == 401 {
				clearCookie(w, r)
				http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
			} else {
				code := access.Status(err)
				key := "ACCESS_DENIED"
				if code == http.StatusServiceUnavailable {
					key = "UPSTREAM_UNAVAILABLE"
				}
				h.fail(w, r, code, key)
			}
			return
		}
		access.Middleware(fixedUser(user), next).ServeHTTP(w, r)
	})
}

type fixedUser string

func (u fixedUser) Authorize(*http.Request) (string, error) { return string(u), nil }
func (h *handler) render(w http.ResponseWriter, r *http.Request, status int, name string, v view) {
	v.Page = name
	user := access.Initiator(r.Context())
	v.Authenticated = user != "" && user != "anonymous"
	var b bytes.Buffer
	if err := h.templates.ExecuteTemplate(&b, "page", v); err != nil {
		http.Error(w, "Ошибка шаблона", 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(b.Bytes())
}
func parseForm(w http.ResponseWriter, r *http.Request) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	return r.ParseForm()
}
func formID() string                { return uuid.NewString() }
func seconds(d time.Duration) int64 { return int64(d / time.Second) }
func duration(raw string) (time.Duration, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 || n > int64((1<<63-1)/time.Second) {
		return 0, invalid("Неверное число секунд")
	}
	return time.Duration(n) * time.Second, nil
}
