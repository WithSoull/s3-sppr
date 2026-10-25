package metadata_test

import (
	"context"
	"errors"
	"testing"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only throttling is given a domain meaning; every other failure reaches the caller unchanged
// so the HTTP layer can map it by gRPC code.
func TestClientMapsOnlyThrottling(t *testing.T) {
	ctx := context.Background()
	upstream := testkit.New()
	metadata, _ := upstream.Clients(t)
	calls := map[string]func() error{
		testkit.OpHeadBucket: func() error {
			_, err := metadata.HeadBucket(ctx, testkit.Bucket)
			return err
		},
		testkit.OpCreateObjectVersion: func() error {
			_, err := metadata.CreateVersion(ctx, testkit.Bucket, "key", "blob", 1, "etag", "text/plain")
			return err
		},
		testkit.OpGetObjectMeta: func() error {
			_, err := metadata.GetObject(ctx, testkit.Bucket, "key")
			return err
		},
	}
	for operation, call := range calls {
		for _, tt := range []struct {
			code           codes.Code
			wantOverloaded bool
		}{
			{codes.ResourceExhausted, true},
			{codes.Unavailable, false},
			{codes.NotFound, false},
			{codes.DeadlineExceeded, false},
		} {
			t.Run(operation+"/"+tt.code.String(), func(t *testing.T) {
				upstream.FailMetadata(status.Error(tt.code, "x"), operation)
				err := call()
				require.Error(t, err)
				require.Equal(t, tt.code, status.Code(err), "gRPC code must stay visible")
				require.Equal(t, tt.wantOverloaded, errors.Is(err, domainerrors.ErrMetadataOverloaded))
			})
		}
	}
}
