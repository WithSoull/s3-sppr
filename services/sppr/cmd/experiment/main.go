package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/runner"
)

func main() {
	config := flag.String("config", "services/sppr/experiments/main.json", "matrix config")
	out := flag.String("out", "build/experiment-main", "new result directory")
	resume := flag.Bool("resume", false, "verify checkpoints and resume with the same executable")
	python := flag.String("python", "", "override Python executable")
	flag.Parse()
	c, err := runner.Load(*config)
	if err == nil {
		if *python != "" {
			c.Simulation.Python = *python
		}
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		err = runner.Run(ctx, c, *out, *resume)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
