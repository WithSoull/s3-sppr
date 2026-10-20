package app

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Real peers verify deadlines added to an ordinary incoming HTTP request.
type hangingMetadata struct {
	metadatav1.UnimplementedMetadataServiceServer
	operation string
	deadlines chan time.Time
	stopped   chan struct{}
}

func (s *hangingMetadata) wait(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	s.deadlines <- deadline
	<-ctx.Done()
	close(s.stopped)
	return ctx.Err()
}

func (s *hangingMetadata) HeadBucket(ctx context.Context, _ *metadatav1.HeadBucketRequest) (*metadatav1.HeadBucketResponse, error) {
	if s.operation == "head" {
		return nil, s.wait(ctx)
	}
	return &metadatav1.HeadBucketResponse{Exists: true}, nil
}

func (s *hangingMetadata) CreateObjectVersion(ctx context.Context, _ *metadatav1.CreateObjectVersionRequest) (*metadatav1.CreateObjectVersionResponse, error) {
	return nil, s.wait(ctx)
}

func (s *hangingMetadata) GetObjectMeta(ctx context.Context, _ *metadatav1.GetObjectMetaRequest) (*metadatav1.GetObjectMetaResponse, error) {
	return nil, s.wait(ctx)
}

type committedStorage struct {
	storagev1.UnimplementedDataStorageServiceServer
}

func (committedStorage) StoreObject(stream storagev1.DataStorageService_StoreObjectServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return stream.SendAndClose(&storagev1.StoreObjectResponse{BlobId: "blob", ChecksumMd5: "etag"})
}

func TestMetadataTimeoutThroughHTTP(t *testing.T) {
	tests := []struct{ name, operation, method string }{
		{name: "HeadBucket", operation: "head", method: http.MethodPut},
		{name: "CreateObjectVersion", operation: "create", method: http.MethodPut},
		{name: "GetObjectMeta", operation: "get", method: http.MethodGet},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			peer := &hangingMetadata{operation: tt.operation, deadlines: make(chan time.Time, 1), stopped: make(chan struct{})}
			grpcServer := grpc.NewServer()
			metadatav1.RegisterMetadataServiceServer(grpcServer, peer)
			storagev1.RegisterDataStorageServiceServer(grpcServer, committedStorage{})
			go grpcServer.Serve(listener)
			t.Cleanup(grpcServer.Stop)
			t.Setenv("METADATA_GRPC_ADDR", listener.Addr().String())
			t.Setenv("STORAGE_GRPC_ADDR", listener.Addr().String())
			t.Setenv("METADATA_RPC_TIMEOUT", "200ms")
			t.Setenv("OTEL_ENABLED", "false")
			t.Setenv("LOGGER_ENABLE_OLTP", "false")
			a, err := NewApp("")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, a.closeResources()) })
			server := httptest.NewServer(a.httpServer.Handler)
			t.Cleanup(server.Close)
			client := server.Client()
			client.Timeout = 3 * time.Second
			// No request deadline is supplied; the server context has none by default.
			request, err := http.NewRequest(tt.method, server.URL+"/objects/bucket/key", strings.NewReader("data"))
			require.NoError(t, err)
			start := time.Now()
			response, err := client.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusGatewayTimeout, response.StatusCode)
			require.Less(t, time.Since(start), 2*time.Second)
			select {
			case deadline := <-peer.deadlines:
				require.False(t, deadline.IsZero())
				require.LessOrEqual(t, deadline.Sub(start), time.Second)
			default:
				t.Fatal("Metadata did not receive the request")
			}
			select {
			case <-peer.stopped:
			case <-time.After(time.Second):
				t.Fatal("Metadata handler survived its RPC deadline")
			}
		})
	}
}
