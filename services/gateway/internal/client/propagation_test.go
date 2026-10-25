package client_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The caller's deadline is shorter than the Metadata timeout, so it must be the one that reaches
// both services over the wire, and Storage must stop streaming once it expires.
func TestCallerDeadlineReachesMetadataAndStorage(t *testing.T) {
	upstream := testkit.New()
	deadlines, stopped := make(chan time.Time, 2), make(chan struct{})
	upstream.OnMetadata = func(ctx context.Context, _ string) error {
		deadline, _ := ctx.Deadline()
		deadlines <- deadline
		return nil
	}
	upstream.BeforeNextChunk = func(ctx context.Context) error {
		deadline, _ := ctx.Deadline()
		deadlines <- deadline
		defer close(stopped)
		<-ctx.Done()
		return ctx.Err()
	}
	upstream.Seed("key", make([]byte, testkit.RetrieveChunkSize+1))
	objects := upstream.ObjectService(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	expected, _ := ctx.Deadline()
	result, err := objects.Get(ctx, testkit.Bucket, "key")
	require.NoError(t, err)
	defer result.Body.Close()

	_, err = io.Copy(io.Discard, result.Body)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	for _, service := range []string{"Metadata", "Storage"} {
		select {
		case actual := <-deadlines:
			require.False(t, actual.IsZero(), "%s got no deadline", service)
			require.WithinDuration(t, expected, actual, 100*time.Millisecond, service)
		default:
			t.Fatalf("%s did not receive the request", service)
		}
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Storage continued after deadline")
	}
}
