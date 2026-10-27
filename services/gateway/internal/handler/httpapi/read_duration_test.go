package httpapi_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

// A client that stops reading fills the TCP buffers and blocks Write; the context
// alone cannot unblock it, so the response needs a write deadline too.
func TestGetToStalledClientEndsAtMaxReadDuration(t *testing.T) {
	const maxReadDuration = 200 * time.Millisecond
	upstream := testkit.New()
	upstream.Seed("key", make([]byte, 64<<20))
	handler := httpapi.NewHandler(upstream.ObjectService(t), maxReadDuration)
	returned := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = io.WriteString(conn, "GET /objects/"+testkit.Bucket+"/key HTTP/1.1\r\nHost: gateway\r\n\r\n")
	require.NoError(t, err)

	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("GET kept writing to a stalled client after the read limit")
	}
}

// The write deadline belongs to one GET; net/http clears it after the request,
// so the next request on a kept-alive connection must not inherit it.
func TestReadLimitDoesNotLeakToNextKeepAliveRequest(t *testing.T) {
	const maxReadDuration = 200 * time.Millisecond
	upstream := testkit.New()
	upstream.Seed("key", []byte("data"))
	server := httptest.NewUnstartedServer(httpapi.NewHandler(upstream.ObjectService(t), maxReadDuration))
	var connections atomic.Int32
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	for range 2 {
		response := do(t, server, http.MethodGet, "/objects/"+testkit.Bucket+"/key", nil)
		require.Equal(t, http.StatusOK, response.StatusCode)
		body, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, "data", string(body))
		time.Sleep(2 * maxReadDuration)
	}
	require.Equal(t, int32(1), connections.Load(), "both requests must share one connection")
}
