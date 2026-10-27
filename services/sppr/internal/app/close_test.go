package app

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/closer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/stretchr/testify/require"
)

type pinger struct{}

func (pinger) Ping(context.Context) error { return nil }

func TestCloseReleasesListenerBeforeRun(t *testing.T) {
	t.Setenv("HTTP_ADDRESS", "127.0.0.1:0")
	httpCfg, err := env.NewHTTPConfig()
	require.NoError(t, err)
	a := &App{cfg: &config.Config{HTTP: httpCfg}, closer: closer.New(&logger.NoopLogger{}, time.Second)}
	require.NoError(t, a.initHTTPServer(pinger{}))

	require.NoError(t, a.Close(context.Background()))
	reopened, err := net.Listen("tcp", a.listener.Addr().String())
	require.NoError(t, err, "Close must release the port even if Run never started")
	require.NoError(t, reopened.Close())
}
