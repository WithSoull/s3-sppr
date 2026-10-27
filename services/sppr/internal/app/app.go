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
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/migration"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/closer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/metric"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/tracing"
	"go.uber.org/zap"
)

type App struct {
	cfg               *config.Config
	closer            *closer.Closer
	provider          *serviceProvider
	consumer          service.AccessConsumerService
	executor          service.MigrationExecutor
	server            *http.Server
	listener          net.Listener
	loggerInitialized bool
}

func NewApp(ctx context.Context, cfg *config.Config) (*App, error) {
	a := &App{cfg: cfg}
	if err := a.initDeps(ctx); err != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout())
		defer cancel()
		return nil, errors.Join(err, a.Close(shutdown))
	}
	return a, nil
}

func (a *App) initDeps(ctx context.Context) error {
	if err := logger.Init(a.cfg.Logger); err != nil {
		return err
	}
	a.loggerInitialized = true
	a.closer = closer.New(logger.Logger(), a.cfg.HTTP.ShutdownTimeout())
	if err := tracing.InitTracer(ctx, a.cfg.Tracing); err != nil {
		return err
	}
	a.closer.AddNamed("tracer", tracing.ShutdownTracer)
	meter, err := metric.InitOTELMetrics(a.cfg.Metrics)
	if err != nil {
		return err
	}
	a.closer.AddNamed("OTEL metrics", meter.Shutdown)

	a.provider = newServiceProvider(a.cfg, a.closer)
	a.consumer, err = a.provider.AccessConsumerService(ctx)
	if err != nil {
		return err
	}
	client, err := a.provider.PGClient(ctx)
	if err != nil {
		return err
	}
	migrations, err := a.provider.MigrationService(ctx)
	if err != nil {
		return err
	}
	// The API answers with the executor disabled, so actions can be queued first.
	if a.cfg.Migration.Enabled() {
		if a.executor, err = a.provider.MigrationExecutor(ctx); err != nil {
			return err
		}
	}
	mux := http.NewServeMux()
	mux.Handle("/", health.NewHandler(client.DB()))
	mux.Handle(migration.Prefix+"/", migration.NewHandler(migrations))
	return a.initHTTPServer(mux)
}

func (a *App) initHTTPServer(handler http.Handler) error {
	a.server = &http.Server{Addr: a.cfg.HTTP.Address(), Handler: handler, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	listener, err := net.Listen("tcp", a.cfg.HTTP.Address())
	if err != nil {
		return err
	}
	a.listener = listener
	// Server.Shutdown only owns the listener after Serve has registered it.
	a.closer.AddNamed("HTTP listener", func(context.Context) error {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil
	})
	return nil
}

// Run serves HTTP, consumes both topics and, when enabled, runs the migration
// executor until ctx is cancelled or the server or consumers fail. Close
// releases the dependencies afterwards.
func (a *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	served := make(chan error, 1)
	consumed := make(chan error, 1)
	executed := make(chan error, 1)
	go func() { served <- a.server.Serve(a.listener) }()
	go func() { consumed <- a.consumer.RunConsumer(ctx) }()
	if a.executor != nil {
		go func() { executed <- a.executor.Run(ctx) }()
	} else {
		executed <- nil
	}
	logger.Info(ctx, "SPPR listening", zap.String("address", a.listener.Addr().String()),
		zap.Bool("migration_enabled", a.executor != nil))

	var result error
	serverDone, consumerDone := false, false
	select {
	case <-ctx.Done():
	case err := <-served:
		serverDone = true
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	case err := <-consumed:
		consumerDone = true
		if err == nil {
			err = errors.New("Kafka consumer stopped unexpectedly")
		}
		result = err
	}
	cancel()

	shutdown, stop := context.WithTimeout(context.Background(), a.cfg.HTTP.ShutdownTimeout())
	defer stop()
	if !serverDone {
		result = errors.Join(result, a.server.Shutdown(shutdown))
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			result = errors.Join(result, err)
		}
	}
	if !consumerDone {
		select {
		case err := <-consumed:
			if !errors.Is(err, context.Canceled) {
				result = errors.Join(result, err)
			}
		case <-shutdown.Done():
			result = errors.Join(result, fmt.Errorf("stop consumers: %w", shutdown.Err()))
		}
	}
	// An interrupted action stays running and is resumed on the next start.
	select {
	case err := <-executed:
		result = errors.Join(result, err)
	case <-shutdown.Done():
		result = errors.Join(result, fmt.Errorf("stop migration workers: %w", shutdown.Err()))
	}
	return result
}

// Close is called after Run has stopped the consumers, so the closer may
// release groups, PostgreSQL and telemetry concurrently. It also handles
// partial initialization.
func (a *App) Close(ctx context.Context) error {
	var err error
	if a.closer != nil {
		err = a.closer.CloseAll(ctx)
	}
	if a.loggerInitialized {
		err = errors.Join(err, logger.Shutdown(ctx))
	}
	return err
}
