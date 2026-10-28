package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/runner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
)

func main() {
	config := flag.String("config", "", "simulation config (defaults: acceptance profile)")
	out := flag.String("out", "build/simulation", "new output directory")
	python := flag.String("python", "", "Python executable")
	scenario := flag.String("scenario", "", "scenario override")
	seed := flag.Uint64("seed", 0, "generator seed")
	flag.Parse()
	c := sim.DefaultConfig()
	var err error
	if *config != "" {
		c, err = sim.ReadConfig(*config)
	}
	if *scenario != "" {
		c.Generator.Scenario = *scenario
	}
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "seed" {
			c.Generator.Seed = *seed
		}
	})
	if *python != "" {
		c.Python = *python
	}
	if err == nil {
		err = os.Mkdir(*out, 0755)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err == nil {
		_, err = sim.Prepare(ctx, c, filepath.Join(*out, "cache"))
	}
	if err == nil {
		var results []sim.Result
		results, err = sim.Replay(ctx, c, filepath.Join(*out, "cache"))
		if err == nil {
			sha, _ := runner.Revision()
			err = sim.WriteResults(filepath.Join(*out, "results"), results, sha)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
