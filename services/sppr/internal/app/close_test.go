package app

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloseReleasesListenerBeforeRun(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	a := &App{provider: &serviceProvider{}, server: &http.Server{}, listener: listener}

	require.NoError(t, a.Close(context.Background()))
	reopened, err := net.Listen("tcp", listener.Addr().String())
	require.NoError(t, err, "Close must release the port even if Run never started")
	require.NoError(t, reopened.Close())
}
