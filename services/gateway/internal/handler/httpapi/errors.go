package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// nginx convention: a client that went away is not an upstream failure.
const statusClientClosedRequest = 499

func writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	code, body := classifyServiceError(err)
	var rangeErr *domainerrors.RangeNotSatisfiableError
	if errors.As(err, &rangeErr) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", rangeErr.Size))
	}
	if r.Context().Err() != nil {
		code, body = statusClientClosedRequest, "client closed request"
	}
	if code >= http.StatusInternalServerError {
		logger.Error(r.Context(), "request failed",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", code),
			zap.Error(err),
		)
		trace.SpanFromContext(r.Context()).RecordError(err)
	}
	http.Error(w, body, code)
}

func classifyServiceError(err error) (int, string) {
	switch {
	case errors.Is(err, domainerrors.ErrBucketNotFound):
		return http.StatusNotFound, "bucket not found"
	case errors.Is(err, domainerrors.ErrBodyRead):
		return http.StatusBadRequest, "could not read request body"
	case errors.Is(err, domainerrors.ErrEmptyStorageStream):
		return http.StatusBadGateway, err.Error()
	case errors.Is(err, domainerrors.ErrInsufficientStorage):
		return http.StatusInsufficientStorage, http.StatusText(http.StatusInsufficientStorage)
	case errors.Is(err, domainerrors.ErrMetadataOverloaded):
		return http.StatusServiceUnavailable, http.StatusText(http.StatusServiceUnavailable)
	case errors.Is(err, domainerrors.ErrBlobMissing):
		return http.StatusBadGateway, http.StatusText(http.StatusBadGateway)
	case errors.Is(err, domainerrors.ErrUnknownStorageClass):
		return http.StatusBadGateway, http.StatusText(http.StatusBadGateway)
	case errors.Is(err, domainerrors.ErrRangeNotSatisfiable):
		return http.StatusRequestedRangeNotSatisfiable, http.StatusText(http.StatusRequestedRangeNotSatisfiable)
	}
	code := http.StatusBadGateway
	switch status.Code(err) {
	case codes.InvalidArgument:
		code = http.StatusBadRequest
	case codes.NotFound:
		code = http.StatusNotFound
	case codes.Unavailable:
		code = http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		code = http.StatusGatewayTimeout
	}
	return code, http.StatusText(code)
}
