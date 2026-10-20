package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteServiceError(t *testing.T) {
	type testCase struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}
	tests := []testCase{
		{name: "missing bucket", err: fmt.Errorf("wrapped: %w", domainerrors.ErrBucketNotFound), wantStatus: http.StatusNotFound, wantBody: "bucket not found\n"},
		{name: "body read", err: &domainerrors.BodyReadError{Err: io.ErrUnexpectedEOF}, wantStatus: http.StatusBadRequest, wantBody: "could not read request body\n"},
		{name: "empty stream", err: domainerrors.ErrEmptyStorageStream, wantStatus: http.StatusBadGateway, wantBody: "storage returned an empty stream\n"},
		{name: "non grpc error", err: errors.New("private detail"), wantStatus: http.StatusBadGateway, wantBody: "Bad Gateway\n"},
		{name: "insufficient storage", err: fmt.Errorf("%w: %w", domainerrors.ErrInsufficientStorage, status.Error(codes.ResourceExhausted, "x")), wantStatus: http.StatusInsufficientStorage, wantBody: "Insufficient Storage\n"},
		{name: "metadata overloaded", err: fmt.Errorf("%w: %w", domainerrors.ErrMetadataOverloaded, status.Error(codes.ResourceExhausted, "x")), wantStatus: http.StatusServiceUnavailable, wantBody: "Service Unavailable\n"},
		{name: "blob missing", err: fmt.Errorf("%w: %w", domainerrors.ErrBlobMissing, status.Error(codes.NotFound, "x")), wantStatus: http.StatusBadGateway, wantBody: "Bad Gateway\n"},
	}
	for _, tc := range []struct {
		code   codes.Code
		status int
	}{
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.NotFound, http.StatusNotFound},
		{codes.ResourceExhausted, http.StatusBadGateway},
		{codes.Unavailable, http.StatusServiceUnavailable},
		{codes.DeadlineExceeded, http.StatusGatewayTimeout},
		{codes.Canceled, http.StatusBadGateway},
		{codes.Unknown, http.StatusBadGateway},
		{codes.AlreadyExists, http.StatusBadGateway},
		{codes.PermissionDenied, http.StatusBadGateway},
		{codes.Unauthenticated, http.StatusBadGateway},
		{codes.FailedPrecondition, http.StatusBadGateway},
		{codes.Aborted, http.StatusBadGateway},
		{codes.OutOfRange, http.StatusBadGateway},
		{codes.Unimplemented, http.StatusBadGateway},
		{codes.Internal, http.StatusBadGateway},
		{codes.DataLoss, http.StatusBadGateway},
	} {
		tests = append(tests, testCase{
			name: tc.code.String(), err: status.Error(tc.code, "private detail"), wantStatus: tc.status, wantBody: http.StatusText(tc.status) + "\n",
		})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeServiceError(response, tt.err)
			require.Equal(t, tt.wantStatus, response.Code)
			require.Equal(t, tt.wantBody, response.Body.String())
		})
	}
}
