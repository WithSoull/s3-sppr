package sim

import (
	"bufio"
	"context"
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/stretchr/testify/require"
)

func TestCacheMatchesFreshFeaturesAndForecasts(t *testing.T) {
	ctx := context.Background()
	c := DefaultConfig()
	c.Generator.Objects = 10
	c.Generator.ReadsPerSecond = 1.0 / 60
	c.Policies = []string{"ewma"}
	c.Budgets = []int64{model.MiB}
	c.UpdateModel = false
	dir := filepath.Join(t.TempDir(), "cache")
	_, err := Prepare(ctx, c, dir)
	require.NoError(t, err)
	cached, err := Replay(ctx, c, dir)
	require.NoError(t, err)
	again, err := Replay(ctx, c, dir)
	require.NoError(t, err)
	for i := range cached {
		a, b := cached[i], again[i]
		a.PlanSeconds, b.PlanSeconds = 0, 0
		require.Equal(t, a, b, "same cached pair must reset placement and queue")
	}
	// Rebuild every test feature/forecast from the same demand, without reading
	// either feature or forecast cache; use the production builder and EWMA.
	demand, err := Generate(ctx, c)
	require.NoError(t, err)
	fresh, err := NewEngine(c, demand.Catalog, demand.Reads, "ewma", model.MiB)
	require.NoError(t, err)
	in, err := os.Open(filepath.Join(dir, "forecasts.gob"))
	require.NoError(t, err)
	defer in.Close()
	dec := gob.NewDecoder(bufio.NewReader(in))
	for at := 72 * time.Hour; at < 168*time.Hour; at += 5 * time.Minute {
		rows, err := demand.Snapshots(ctx, c, at)
		require.NoError(t, err)
		f := Frame{At: int64(at), Features: make([]model.Features, len(rows)), Forecasts: map[string][]float64{"ewma": make([]float64, len(rows))}, ModelVersions: map[string]string{"ewma": ""}}
		for i, s := range rows {
			p, err := ewma.Predict(s, time.Hour)
			require.NoError(t, err)
			f.Features[i] = s.Features
			f.Forecasts["ewma"][i] = p.ExpectedReads
		}
		var stored Frame
		require.NoError(t, dec.Decode(&stored))
		require.Equal(t, f.Features, stored.Features)
		require.Equal(t, f.Forecasts, stored.Forecasts)
		require.NoError(t, fresh.Plan(f))
	}
	var extra Frame
	require.ErrorIs(t, dec.Decode(&extra), io.EOF)
	got, err := fresh.Finish()
	require.NoError(t, err)
	got.ForecastMAE = cached[0].ForecastMAE
	got.PlanSeconds = 0
	cached[0].PlanSeconds = 0
	require.Equal(t, cached[0], got)
	// Changing a demand-defining input or corrupting a cache is a hard error.
	changed := c
	changed.ReadFraction = .5
	_, err = Replay(ctx, changed, dir)
	require.ErrorContains(t, err, "incompatible cache")
	f, err := os.OpenFile(filepath.Join(dir, "forecasts.gob"), os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString("broken")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = Replay(ctx, c, dir)
	require.ErrorContains(t, err, "cache hash")
}
