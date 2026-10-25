package httpapi_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// End-to-end failure paths: a gRPC failure in a fake downstream service must reach the HTTP client
// as the right status, and a failed PUT must leave only what the wantBlobs/wantVersions columns say.
// The status mapping itself is pinned once, in TestWriteServiceError.
func TestUpstreamFailures(t *testing.T) {
	throttled := status.Error(codes.ResourceExhausted, "too many requests")
	tests := []struct {
		name         string
		method       string
		bucket       string // testkit.Bucket when empty
		body         io.Reader
		prepare      func(*testkit.Upstream)
		wantStatus   int
		wantBlobs    int // PUT only
		wantVersions int // PUT only
	}{
		{name: "put: missing bucket", method: http.MethodPut, bucket: "missing", wantStatus: http.StatusNotFound},
		{
			// The body is larger than the Gateway reads before it learns that Storage refused.
			name:       "put: storage full while streaming",
			method:     http.MethodPut,
			body:       strings.NewReader(strings.Repeat("x", 8*testkit.RetrieveChunkSize)),
			prepare:    func(u *testkit.Upstream) { u.FailStore = status.Error(codes.ResourceExhausted, "disk full") },
			wantStatus: http.StatusInsufficientStorage,
		},
		{
			name:       "put: metadata throttled",
			method:     http.MethodPut,
			prepare:    func(u *testkit.Upstream) { u.FailMetadata(throttled) },
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:   "put: version registration fails after commit",
			method: http.MethodPut,
			prepare: func(u *testkit.Upstream) {
				u.FailMetadata(status.Error(codes.Internal, "boom"), testkit.OpCreateObjectVersion)
			},
			wantStatus: http.StatusBadGateway,
			wantBlobs:  1,
		},
		{name: "get: missing object", method: http.MethodGet, wantStatus: http.StatusNotFound},
		{
			name:       "get: metadata throttled",
			method:     http.MethodGet,
			prepare:    func(u *testkit.Upstream) { u.FailMetadata(throttled) },
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:   "get: blob missing in storage",
			method: http.MethodGet,
			prepare: func(u *testkit.Upstream) {
				u.Meta["key"] = &metadatav1.GetObjectMetaResponse{BlobId: "missing", SizeBytes: 4}
			},
			wantStatus: http.StatusBadGateway,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, upstream := newGateway(t)
			if tt.prepare != nil {
				tt.prepare(upstream)
			}
			body := tt.body
			if body == nil && tt.method == http.MethodPut {
				body = strings.NewReader("data")
			}
			bucket := tt.bucket
			if bucket == "" {
				bucket = testkit.Bucket
			}
			response := do(t, server, tt.method, "/objects/"+bucket+"/key", body)
			require.Equal(t, tt.wantStatus, response.StatusCode)
			if tt.method == http.MethodPut {
				blobs, versions := upstream.Stats()
				require.Equal(t, tt.wantBlobs, blobs, "stored blobs")
				require.Equal(t, tt.wantVersions, versions, "published versions")
			}
		})
	}
}
