package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/app"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/config"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	path := flag.String("config-path", ".env", "path to optional environment file")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	a, err := app.NewApp(ctx, cfg)
	if err != nil {
		return fmt.Errorf("initialize SPPR: %w", err)
	}
	logger.Info(ctx, "SPPR ingestion started")
	runErr := a.Run(ctx)
	shutdown, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout())
	defer cancel()
	closeErr := a.Close(shutdown)
	return errors.Join(runErr, closeErr)
}
