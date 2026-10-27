package httpapi_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
)

// A migration deletes the source blob only after GATEWAY_MAX_READ_DURATION, so
// no GET may keep reading from Storage longer than that.
func TestGetIsCutAtMaxReadDuration(t *testing.T) {
	const maxReadDuration = 200 * time.Millisecond
	upstream := testkit.New()
	server := httptest.NewServer(httpapi.NewHandler(upstream.ObjectService(t), maxReadDuration))
	t.Cleanup(server.Close)

	metadataDeadline := make(chan time.Time, 1)
	upstream.OnMetadata = func(ctx context.Context, _ string) error {
		deadline, _ := ctx.Deadline()
		metadataDeadline <- deadline
		return nil
	}
	storageStopped := make(chan struct{})
	upstream.BeforeNextChunk = func(ctx context.Context) error {
		defer close(storageStopped)
		<-ctx.Done()
		return ctx.Err()
	}
	upstream.Seed("key", make([]byte, testkit.RetrieveChunkSize+1))

	// The client gives up much later than the limit, so a hang fails fast instead of blocking the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/objects/"+testkit.Bucket+"/key", nil)
	require.NoError(t, err)
	start := time.Now()
	response, err := server.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode, "headers are sent before the stream stalls")
	_, err = io.ReadAll(response.Body)
	require.Error(t, err, "a cut response must not look complete")

	select {
	case <-storageStopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Storage stream outlived the read limit")
	}
	require.Less(t, time.Since(start), 2*time.Second)
	require.WithinDuration(t, start.Add(maxReadDuration), <-metadataDeadline, 100*time.Millisecond,
		"Metadata lookup must count toward the same limit")
}
