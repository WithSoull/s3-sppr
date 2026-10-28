package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
	"github.com/stretchr/testify/require"
)

func smallConfig() Config {
	c := Config{Mode: "sim", Workers: 1, Scenarios: []string{"stable"}, Seeds: []uint64{0}, Simulation: sim.DefaultConfig()}
	c.Simulation.Generator.Objects = 10
	c.Simulation.Generator.ReadsPerSecond = 1.0 / 60
	c.Simulation.Policies = []string{"ewma"}
	c.Simulation.Budgets = []int64{1 << 20}
	c.Simulation.UpdateModel = false
	return c
}
func TestResumeAndRefuseOverwrite(t *testing.T) {
	t.Setenv("SPPR_GIT_SHA", "test-revision")
	out := filepath.Join(t.TempDir(), "series")
	c := smallConfig()
	ctx := context.Background()
	require.NoError(t, Run(ctx, c, out, false))
	require.ErrorContains(t, Run(ctx, c, out, false), "new series required")
	path := filepath.Join(out, "pairs", "stable-0", "results", "ewma-1048576.json")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	// A checkpoint remains byte-identical on resume, including plan timings.
	require.NoError(t, Run(ctx, c, out, true))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	changed := c
	changed.Simulation.Slots = 7
	require.ErrorContains(t, Run(ctx, changed, out, true), "same configuration")
	require.NoError(t, os.WriteFile(path, []byte("{}"), 0644))
	require.ErrorContains(t, Run(ctx, c, out, true), "checkpoint hashes differ")
}
func TestInterruptedPairRecomputed(t *testing.T) {
	t.Setenv("SPPR_GIT_SHA", "test-revision")
	out := filepath.Join(t.TempDir(), "series")
	c := smallConfig()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, Run(ctx, c, out, false))
	partial := filepath.Join(out, "pairs", "stable-0.partial")
	require.NoError(t, os.MkdirAll(partial, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(partial, "unfinished"), []byte("x"), 0644))
	require.NoError(t, Run(context.Background(), c, out, true))
	_, err := os.Stat(partial)
	require.True(t, os.IsNotExist(err))
}
func TestConfigValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Config)
	}{
		{"stand", func(c *Config) { c.Mode = "stand" }}, {"duplicate seeds", func(c *Config) { c.Seeds = []uint64{1, 1} }}, {"duplicate scenarios", func(c *Config) { c.Scenarios = []string{"stable", "stable"} }}, {"invalid cycle", func(c *Config) { c.Simulation.CycleSeconds = 7 }},
	} {
		t.Run(tt.name, func(t *testing.T) { c := smallConfig(); tt.mutate(&c); require.Error(t, c.Validate()) })
	}
}
