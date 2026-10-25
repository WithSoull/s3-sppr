package httpapi_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

// A response that has started with 200 can only be cut short, never turned into an error status.
func TestGetAbortsIncompleteResponse(t *testing.T) {
	for _, tc := range []struct {
		name string
		body io.Reader
		size int64
	}{
		{"short clean EOF", strings.NewReader("abc"), 6},
		{"failure after declared bytes", io.MultiReader(strings.NewReader("abc"), failedReader{}), 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger.SetNopLogger()
			mc := minimock.NewController(t)
			req := httptest.NewRequest(http.MethodGet, "/objects/bucket/key", nil)
			body := &trackedBody{Reader: tc.body}
			objects := mocks.NewObjectServiceMock(mc)
			objects.GetMock.Expect(req.Context(), "bucket", "key").Return(model.GetResult{Body: body, Size: tc.size}, nil)
			handler := httpapi.NewHandler(objects)
			defer func() {
				require.Equal(t, 1, body.closes)
				if got := recover(); got != http.ErrAbortHandler {
					t.Fatalf("panic = %v, want http.ErrAbortHandler", got)
				}
			}()
			handler.ServeHTTP(httptest.NewRecorder(), req)
		})
	}
}

func TestGetStreamsBeforeStorageEOF(t *testing.T) {
	server, upstream := newGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	next := make(chan struct{})
	upstream.BeforeNextChunk = func(ctx context.Context) error {
		select {
		case <-next:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	const size = 9 * 1024 * 1024
	upstream.Seed("stream", []byte(strings.Repeat("x", size)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/objects/fixture/stream", nil)
	require.NoError(t, err)
	response, err := server.Client().Do(req)
	require.NoError(t, err, "Gateway did not return a prefix before Storage EOF")
	defer response.Body.Close()
	first := make([]byte, 1)
	_, err = io.ReadFull(response.Body, first)
	require.NoError(t, err)
	require.Equal(t, byte('x'), first[0])
	close(next)
	n, err := io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.EqualValues(t, size-1, n)
}

func TestClientCancellationStopsStorageRead(t *testing.T) {
	server, upstream := newGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped := make(chan struct{})
	upstream.BeforeNextChunk = func(ctx context.Context) error {
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}
	upstream.Seed("key", []byte(strings.Repeat("x", 9*1024*1024)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/objects/fixture/key", nil)
	require.NoError(t, err)
	response, err := server.Client().Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	cancel()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Storage stream survived HTTP client cancellation")
	}
}

// What Metadata declares and what Storage streams disagree. The Gateway has not sent a status yet
// for an empty stream, and has for an oversized one, where the client sees a broken body.
func TestGetStorageDisagreesWithMetadata(t *testing.T) {
	tests := []struct {
		name          string
		storedSize    int
		declaredSize  int64
		wantStatus    int
		wantReadError bool
	}{
		{name: "oversized stream", storedSize: 131073, declaredSize: 131072, wantStatus: http.StatusOK, wantReadError: true},
		{name: "empty stream for nonempty object", declaredSize: 1, wantStatus: http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, upstream := newGateway(t)
			upstream.Blobs["blob"] = []byte(strings.Repeat("x", tt.storedSize))
			upstream.Meta["key"] = &metadatav1.GetObjectMetaResponse{BlobId: "blob", SizeBytes: tt.declaredSize}
			response := do(t, server, http.MethodGet, "/objects/"+testkit.Bucket+"/key", nil)
			require.Equal(t, tt.wantStatus, response.StatusCode)
			body, err := io.ReadAll(response.Body)
			if tt.wantReadError {
				require.Error(t, err)
				require.Less(t, int64(len(body)), tt.declaredSize)
			}
		})
	}
}

func TestEmptyObjectWaitsForStorageCompletion(t *testing.T) {
	logger.SetNopLogger()
	mc := minimock.NewController(t)
	objects := mocks.NewObjectServiceMock(mc)
	objects.GetMock.Set(func(_ context.Context, bucket, key string) (model.GetResult, error) {
		require.Equal(t, "bucket", bucket)
		require.Equal(t, "key", key)
		return model.GetResult{Body: io.NopCloser(failedReader{}), Size: 0}, nil
	})
	server := httptest.NewServer(httpapi.NewHandler(objects))
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/objects/bucket/key")
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusBadGateway, response.StatusCode)
}
