package httpapi_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUpstreamErrorsMapToHTTPStatus(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		prepare    func(*fixture)
		wantStatus int
	}{
		{
			name:       "metadata throttled on put",
			method:     http.MethodPut,
			prepare:    func(s *fixture) { s.failMetadata = status.Error(codes.ResourceExhausted, "to many requests") },
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "metadata throttled on get",
			method:     http.MethodGet,
			prepare:    func(s *fixture) { s.failMetadata = status.Error(codes.ResourceExhausted, "to many requests") },
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "storage full on put",
			method:     http.MethodPut,
			prepare:    func(s *fixture) { s.failStore = status.Error(codes.ResourceExhausted, "disk full") },
			wantStatus: http.StatusInsufficientStorage,
		},
		{
			name:   "blob missing on get",
			method: http.MethodGet,
			prepare: func(s *fixture) {
				s.meta["key"] = &metadatav1.GetObjectMetaResponse{BlobId: "missing", SizeBytes: 4}
			},
			wantStatus: http.StatusBadGateway,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, state := newGateway(t)
			state.Lock()
			tt.prepare(state)
			state.Unlock()

			var body io.Reader
			if tt.method == http.MethodPut {
				body = strings.NewReader("data")
			}
			request, err := http.NewRequest(tt.method, server.URL+"/objects/fixture/key", body)
			require.NoError(t, err)
			response, err := server.Client().Do(request)
			require.NoError(t, err)
			response.Body.Close()
			require.Equal(t, tt.wantStatus, response.StatusCode)

			if tt.name == "metadata throttled on put" {
				state.Lock()
				defer state.Unlock()
				require.Zero(t, state.storeCount)
			}
		})
	}
}
