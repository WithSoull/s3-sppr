package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
	"github.com/stretchr/testify/require"
)

func tinyConfig() synthetic.Config {
	return synthetic.Config{Scenario: "stable", Seed: 0, Start: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Objects: 20, SizeBytes: 1 << 20,
		ReadsPerSecond: .01, SnapshotSeconds: 300, HotFraction: .1, HotProbability: .8, DecayHours: 24, PeriodHours: 24, BurstAtHours: 100, BurstDurationHours: .5, ShiftEveryHours: 24}
}
func TestSmallCatalogue(t *testing.T) {
	for _, tt := range []struct {
		name, model string
		methods     []string
	}{
		{"EWMA", "", []string{"ewma-1h-v1"}},
		{"EWMA and ML", "../../internal/service/predictor/lgbm/testdata", []string{"ewma-1h-v1", "lgbm-hurdle-v1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := tinyConfig()
			raw, err := json.Marshal(cfg)
			require.NoError(t, err)
			config := filepath.Join(dir, "config.json")
			require.NoError(t, os.WriteFile(config, raw, 0600))
			out := filepath.Join(dir, "result.json")
			args := []string{"-config", config, "-objects", "20", "-out", out}
			if tt.model != "" {
				args = append(args, "-model", tt.model)
			}
			require.NoError(t, run(args))
			raw, err = os.ReadFile(out)
			require.NoError(t, err)
			var r report
			require.NoError(t, json.Unmarshal(raw, &r))
			require.Equal(t, 20, r.Generator.Objects)
			require.Equal(t, "bench-plan-v1", r.Schema)
			require.Greater(t, r.HistoryRows, 0)
			require.Len(t, r.Cycles, len(tt.methods))
			for i, c := range r.Cycles {
				require.Equal(t, tt.methods[i], c.Method)
				require.Equal(t, 20, c.Rows)
				require.Equal(t, 20, c.Forecasts)
				require.Greater(t, c.Seconds, 0.0)
				require.GreaterOrEqual(t, c.HeapPeak, c.HeapBase)
				require.Greater(t, c.MaxRSS, uint64(0))
				require.Greater(t, c.Summary.Actions, 0)
			}
			require.ErrorContains(t, run(args), "output exists")
		})
	}
}
func TestPrepareDeterministicAndCausal(t *testing.T) {
	cfg := tinyConfig()
	a, err := prepare(context.Background(), cfg)
	require.NoError(t, err)
	b, err := prepare(context.Background(), cfg)
	require.NoError(t, err)
	require.Equal(t, a, b)
	var reads int64
	for _, r := range a.Reads {
		require.True(t, r.Minute.Before(a.At))
		reads += r.Reads
	}
	require.EqualValues(t, 864, reads)
	require.Equal(t, cfg.Start.Add(24*time.Hour), a.Coverage.Watermark)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = prepare(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
}
