package component

import (
	"context"
	"testing"

	desc "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func storeWithBlobID(ctx context.Context, blobID string, data []byte, size int64) (*desc.StoreObjectResponse, error) {
	stream, err := client.StoreObject(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&desc.StoreObjectRequest{Payload: &desc.StoreObjectRequest_Header{
		Header: &desc.StoreObjectHeader{Size: &size, BlobId: blobID, Data: data},
	}}); err != nil {
		return nil, err
	}
	return stream.CloseAndRecv()
}

// A migration retry writes under the same blob_id; a rejected retry must not
// destroy the copy that is already there.
func TestStoreWithBlobID_RejectedRetryKeepsExistingBlob(t *testing.T) {
	ctx := context.Background()
	blobID := uuid.NewString()

	stored, err := storeWithBlobID(ctx, blobID, []byte("good"), 4)
	require.NoError(t, err)
	require.Equal(t, blobID, stored.GetBlobId())

	_, err = storeWithBlobID(ctx, blobID, []byte("x"), 4)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	data, _ := retrieveBlob(t, ctx, blobID, 0, 0)
	require.Equal(t, []byte("good"), data)
}

func TestStoreWithBlobID_RetryReplacesBlob(t *testing.T) {
	ctx := context.Background()
	blobID := uuid.NewString()

	_, err := storeWithBlobID(ctx, blobID, []byte("first"), 5)
	require.NoError(t, err)
	_, err = storeWithBlobID(ctx, blobID, []byte("again"), 5)
	require.NoError(t, err)

	data, _ := retrieveBlob(t, ctx, blobID, 0, 0)
	require.Equal(t, []byte("again"), data)
}
