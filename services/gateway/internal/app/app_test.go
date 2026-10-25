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

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"
)

// newTestApp builds the real App on a free loopback port. Metadata and Storage point at a closed
// port unless env overrides them; env wins over every default.
func newTestApp(t *testing.T, env map[string]string) *App {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	settings := map[string]string{
		"GATEWAY_HTTP_ADDR":  address,
		"METADATA_GRPC_ADDR": "127.0.0.1:1",
		"STORAGE_GRPC_ADDR":  "127.0.0.1:1",
		"OTEL_ENABLED":       "false",
		"LOGGER_ENABLE_OLTP": "false",
	}
	for key, value := range env {
		settings[key] = value
	}
	for key, value := range settings {
		t.Setenv(key, value)
	}
	a, err := NewApp("")
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.serviceProvider.Close() })
	return a
}

func TestRunReturnsListenError(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	a := newTestApp(t, map[string]string{"GATEWAY_HTTP_ADDR": occupied.Addr().String()})
	require.Error(t, a.Run(context.Background()), "Run() should report occupied address")
}

func TestConfiguredClientsAreReused(t *testing.T) {
	a := newTestApp(t, nil)
	m1, err := a.serviceProvider.MetadataClient()
	require.NoError(t, err)
	m2, err := a.serviceProvider.MetadataClient()
	require.NoError(t, err)
	s1, err := a.serviceProvider.StorageClient()
	require.NoError(t, err)
	s2, err := a.serviceProvider.StorageClient()
	require.NoError(t, err)
	require.True(t, m1 == m2 && s1 == s2, "downstream clients are not reused")
	require.Nil(t, a.serviceProvider.accessPublisher)
	require.Nil(t, a.serviceProvider.accessTransport)
	require.Nil(t, a.serviceProvider.accessClient)
	require.Equal(t, a.config.Metadata.Address(), a.serviceProvider.metadataConn.Target())
	require.Equal(t, a.config.Storage.Address(), a.serviceProvider.storageConn.Target())
}

func TestUnavailableMetadataDoesNotReturnHTTPSuccess(t *testing.T) {
	a := newTestApp(t, nil)
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			req := httptest.NewRequest(method, "/objects/fixture/key", strings.NewReader("data")).WithContext(ctx)
			response := httptest.NewRecorder()
			a.httpServer.Handler.ServeHTTP(response, req)
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
		})
	}
}

// Run must start serving, keep an in-flight request alive after the context is cancelled,
// and only then close the downstream gRPC connections.
func TestShutdownDrainsActiveRequestBeforeClosingClients(t *testing.T) {
	a := newTestApp(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	a.httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "completed")
	})
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	url := "http://" + a.config.HTTP.Address()
	require.Eventually(t, func() bool {
		response, err := client.Get(url + "/healthz")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 3*time.Second, 10*time.Millisecond, "Gateway did not start")

	requestDone := make(chan error, 1)
	go func() {
		response, err := client.Get(url + "/object")
		if err == nil {
			defer response.Body.Close()
			var body []byte
			body, err = io.ReadAll(response.Body)
			if err == nil && string(body) != "completed" {
				err = io.ErrUnexpectedEOF
			}
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before request completion: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NotEqual(t, connectivity.Shutdown, a.serviceProvider.storageConn.GetState(), "Storage client closed before drain")
	require.NotEqual(t, connectivity.Shutdown, a.serviceProvider.metadataConn.GetState(), "Metadata client closed before drain")
	close(release)
	select {
	case err := <-requestDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("active request did not finish")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	require.Equal(t, connectivity.Shutdown, a.serviceProvider.storageConn.GetState(), "Storage client not closed")
	require.Equal(t, connectivity.Shutdown, a.serviceProvider.metadataConn.GetState(), "Metadata client not closed")
}
