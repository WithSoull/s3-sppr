package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

const (
	Prefix       = "/api/migration"
	maxBodyBytes = 4 << 20
)

type handler struct{ service service.MigrationService }

// NewHandler serves the migration API under Prefix; the app wraps it in access.Middleware.
func NewHandler(s service.MigrationService) http.Handler {
	h := &handler{service: s}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+Prefix+"/actions", h.submit)
	mux.HandleFunc("GET "+Prefix+"/actions", h.list)
	mux.HandleFunc("GET "+Prefix+"/actions/{id}", h.get)
	mux.HandleFunc("POST "+Prefix+"/actions/cancel", h.cancel)
	mux.HandleFunc("POST "+Prefix+"/pause", h.pause(true))
	mux.HandleFunc("POST "+Prefix+"/resume", h.pause(false))
	mux.HandleFunc("GET "+Prefix+"/budget", h.getBudget)
	mux.HandleFunc("PUT "+Prefix+"/budget", h.putBudget)
	mux.HandleFunc("GET "+Prefix+"/settings", h.getSettings)
	mux.HandleFunc("PUT "+Prefix+"/settings", h.putSettings)
	mux.HandleFunc("GET "+Prefix+"/pins", h.listPins)
	mux.HandleFunc("POST "+Prefix+"/pins", h.addPin)
	mux.HandleFunc("DELETE "+Prefix+"/pins", h.deletePin)
	return mux
}

func decode(r *http.Request, dst any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("%w: request body: %v", model.ErrInvalidArgument, err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(ctx context.Context, w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, model.ErrInvalidArgument):
		status = http.StatusBadRequest
	case errors.Is(err, model.ErrActionNotFound), errors.Is(err, model.ErrPinNotFound):
		status = http.StatusNotFound
	case errors.Is(err, model.ErrActionConflict), errors.Is(err, model.ErrPinExists):
		status = http.StatusConflict
	}
	message := err.Error()
	if status == http.StatusInternalServerError {
		logger.Error(ctx, "migration API request failed", zap.Error(err))
		message = "internal error"
	}
	writeJSON(w, status, errorResponse{Error: message})
}
