package metadata_test

import (
	"context"
	"net"
	"testing"
	"time"

	metadataClient "github.com/alesplll/opens3-rebac/services/sppr/internal/client/metadata"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type cleanupServer struct {
	metadatav1.UnimplementedPlacementServiceServer
	list     func(context.Context, *metadatav1.ListDueCleanupsRequest) (*metadatav1.ListDueCleanupsResponse, error)
	complete func(context.Context, *metadatav1.CompleteCleanupRequest) (*metadatav1.CompleteCleanupResponse, error)
}

func (s *cleanupServer) ListDueCleanups(ctx context.Context, r *metadatav1.ListDueCleanupsRequest) (*metadatav1.ListDueCleanupsResponse, error) {
	return s.list(ctx, r)
}
func (s *cleanupServer) CompleteCleanup(ctx context.Context, r *metadatav1.CompleteCleanupRequest) (*metadatav1.CompleteCleanupResponse, error) {
	return s.complete(ctx, r)
}
func connection(t *testing.T, api *cleanupServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	metadatav1.RegisterPlacementServiceServer(server, api)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///metadata", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}
func TestListDueCleanups(t *testing.T) {
	cases := []struct {
		name    string
		rpcErr  error
		wantErr error
	}{{name: "orphan mapping"}, {name: "unavailable", rpcErr: status.Error(codes.Unavailable, "down"), wantErr: model.ErrUpstreamUnavailable}}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			api := &cleanupServer{list: func(ctx context.Context, r *metadatav1.ListDueCleanupsRequest) (*metadatav1.ListDueCleanupsResponse, error) {
				require.Equal(t, int32(100), r.GetLimit())
				if tt.rpcErr != nil {
					return nil, tt.rpcErr
				}
				return &metadatav1.ListDueCleanupsResponse{Copies: []*metadatav1.PlacementCopyInfo{{CopyId: "copy", Role: "orphan", StorageClass: "COLD", BlobId: "blob", State: "cleanup_pending"}}}, nil
			}}
			c := metadataClient.NewClient(metadatav1.NewPlacementServiceClient(connection(t, api)), time.Second)
			copies, err := c.ListDueCleanups(context.Background(), 100)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []model.PlacementCopy{{CopyID: "copy", Role: model.CopyRoleOrphan, Class: model.StorageClassCold, BlobID: "blob", State: model.CopyCleanupPending}}, copies)
		})
	}
}
func TestCompleteCleanup(t *testing.T) {
	for _, deleted := range []bool{true, false} {
		t.Run(map[bool]string{true: "deleted", false: "failed"}[deleted], func(t *testing.T) {
			message := ""
			if !deleted {
				message = "disk error"
			}
			api := &cleanupServer{complete: func(ctx context.Context, r *metadatav1.CompleteCleanupRequest) (*metadatav1.CompleteCleanupResponse, error) {
				require.Equal(t, "copy", r.GetCopyId())
				require.Equal(t, deleted, r.GetDeleted())
				require.Equal(t, message, r.GetErrorMessage())
				return &metadatav1.CompleteCleanupResponse{}, nil
			}}
			c := metadataClient.NewClient(metadatav1.NewPlacementServiceClient(connection(t, api)), time.Second)
			require.NoError(t, c.CompleteCleanup(context.Background(), "copy", deleted, message))
		})
	}
}
func TestCleanupDeadline(t *testing.T) {
	api := &cleanupServer{list: func(ctx context.Context, _ *metadatav1.ListDueCleanupsRequest) (*metadatav1.ListDueCleanupsResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	c := metadataClient.NewClient(metadatav1.NewPlacementServiceClient(connection(t, api)), 20*time.Millisecond)
	_, err := c.ListDueCleanups(context.Background(), 100)
	require.ErrorIs(t, err, model.ErrUpstreamUnavailable)
}
