package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/health"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/metric"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/tracing"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type App struct {
	provider           *serviceProvider
	server             *http.Server
	listener           net.Listener
	meter              *sdkmetric.MeterProvider
	loggerInitialized  bool
	tracingInitialized bool
}

func NewApp(ctx context.Context, cfg *config.Config) (_ *App, err error) {
	a := &App{provider: newServiceProvider(cfg)}
	defer func() {
		if err != nil {
			shutdown, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout())
			defer cancel()
			err = errors.Join(err, a.Close(shutdown))
		}
	}()
	if err = logger.Init(cfg.Logger); err != nil {
		return nil, err
	}
	a.loggerInitialized = true
	if err = tracing.InitTracer(ctx, cfg.Tracing); err != nil {
		return nil, err
	}
	a.tracingInitialized = true
	a.meter, err = metric.InitOTELMetrics(cfg.Metrics)
	if err != nil {
		return nil, err
	}
	worker, err := a.provider.Worker(ctx)
	if err != nil {
		return nil, err
	}
	client, err := a.provider.PGClient(ctx)
	if err != nil {
		return nil, err
	}
	a.server = &http.Server{Addr: cfg.HTTP.Address(), Handler: health.NewHandler(client.DB(), worker), ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	a.listener, err = net.Listen("tcp", cfg.HTTP.Address())
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (a *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- a.server.Serve(a.listener) }()
	go func() { results <- a.provider.worker.Run(ctx) }()
	var result error
	finished := 0
	select {
	case <-ctx.Done():
	case result = <-results:
		finished++
		if errors.Is(result, http.ErrServerClosed) {
			result = nil
		}
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), a.provider.cfg.HTTP.ShutdownTimeout())
	defer stop()
	result = errors.Join(result, a.server.Shutdown(shutdown))
	for finished < 2 {
		select {
		case err := <-results:
			finished++
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				result = errors.Join(result, err)
			}
		case <-shutdown.Done():
			return errors.Join(result, fmt.Errorf("stop workers: %w", shutdown.Err()))
		}
	}
	return result
}

// Close is called after Run has stopped workers. Dependency cleanup is ordered:
// consumer group, database, telemetry. It also handles partial initialization.
func (a *App) Close(ctx context.Context) error {
	var err error
	if a.server != nil {
		err = errors.Join(err, a.server.Close())
	}
	// Server.Close only owns the listener after Serve has registered it.
	if a.listener != nil {
		if closeErr := a.listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}
	if a.provider.group != nil {
		err = errors.Join(err, a.provider.group.Close())
	}
	if a.provider.pgClient != nil {
		err = errors.Join(err, a.provider.pgClient.Close())
	}
	if a.meter != nil {
		err = errors.Join(err, a.meter.Shutdown(ctx))
	}
	if a.tracingInitialized {
		err = errors.Join(err, tracing.ShutdownTracer(ctx))
	}
	if a.loggerInitialized {
		err = errors.Join(err, logger.Shutdown(ctx))
	}
	return err
}
