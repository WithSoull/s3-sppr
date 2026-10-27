package httpapi

import (
	"io"
	"net/http"
	"strings"
	"time"

	_ "github.com/alesplll/opens3-rebac/services/gateway/docs"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

type Handler struct {
	objects         service.ObjectService
	maxReadDuration time.Duration
	swagger         http.Handler
}

func NewHandler(objects service.ObjectService, maxReadDuration time.Duration) http.Handler {
	return &Handler{
		objects:         objects,
		maxReadDuration: maxReadDuration,
		swagger:         httpSwagger.Handler(httpSwagger.URL("/swagger/doc.json")),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/swagger/") {
		h.swagger.ServeHTTP(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut:
		h.PutObject(w, r)
	case http.MethodGet:
		h.GetObject(w, r)
	case http.MethodDelete:
		h.DeleteObject(w, r)
	default:
		if _, _, ok := objectPath(r.URL.Path); !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func objectPath(path string) (bucket, key string, ok bool) {
	if !strings.HasPrefix(path, "/objects/") {
		return "", "", false
	}
	bucket, key, ok = strings.Cut(strings.TrimPrefix(path, "/objects/"), "/")
	return bucket, key, ok && bucket != "" && key != ""
}
