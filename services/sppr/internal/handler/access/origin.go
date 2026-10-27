package access

import (
	"net/http"
	"net/url"
	"strings"
)

// SameSite cookies complement this check. Bearer clients may omit Origin and
// Referer, but a supplied foreign host is always refused, also on login.
func SameOrigin(r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
		return true
	}
	for _, key := range []string{"Origin", "Referer"} {
		if raw := r.Header.Get(key); raw != "" {
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || !strings.EqualFold(u.Host, r.Host) {
				return false
			}
		}
	}
	return true
}
