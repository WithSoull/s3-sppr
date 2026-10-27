package storage_test

import (
	"context"
	"net"
	"testing"

	storageClient "github.com/alesplll/opens3-rebac/services/sppr/internal/client/storage"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type deleteServer struct {
	storagev1.UnimplementedDataStorageServiceServer
	success bool
	err     error
}

func (s *deleteServer) DeleteObject(ctx context.Context, r *storagev1.DeleteObjectRequest) (*storagev1.DeleteObjectResponse, error) {
	return &storagev1.DeleteObjectResponse{Success: s.success}, s.err
}
func TestDelete(t *testing.T) {
	cases := []struct {
		name        string
		success     bool
		rpcErr      error
		wantErr     error
		wantMessage string
	}{
		{name: "deleted or already missing", success: true},
		{name: "unavailable", rpcErr: status.Error(codes.Unavailable, "down"), wantErr: model.ErrUpstreamUnavailable},
		{name: "false confirmation", wantMessage: "did not confirm deletion"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			storagev1.RegisterDataStorageServiceServer(server, &deleteServer{success: tt.success, err: tt.rpcErr})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient("passthrough:///storage", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			require.NoError(t, err)
			defer conn.Close()
			err = storageClient.NewClient(storagev1.NewDataStorageServiceClient(conn)).Delete(context.Background(), "blob")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else if tt.wantMessage != "" {
				require.ErrorContains(t, err, tt.wantMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
