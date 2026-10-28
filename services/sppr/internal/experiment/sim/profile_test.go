package sim

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Optional diagnostic on a previously prepared acceptance cache; generation and
// training are excluded. The full measurement still uses cmd/experiment.
func TestCachedPlanningScale(t *testing.T) {
	dir := os.Getenv("SPPR_SIM_PROFILE")
	if dir == "" {
		t.Skip("set SPPR_SIM_PROFILE to a prepared full-size cache")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	var manifest CacheManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))
	c := manifest.Config
	var d Demand
	require.NoError(t, readGob(filepath.Join(dir, "demand.gob"), &d))
	d.Minutes = nil
	var engines []*Engine
	for _, p := range c.Policies {
		for _, b := range c.Budgets {
			e, err := NewEngine(c, d.Catalog, d.Reads, p, b)
			require.NoError(t, err)
			engines = append(engines, e)
		}
	}
	f, err := os.Open(filepath.Join(dir, "forecasts.gob"))
	require.NoError(t, err)
	defer f.Close()
	dec := gob.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	started := time.Now()
	for n := 0; n < 12; n++ {
		var frame Frame
		require.NoError(t, dec.Decode(&frame))
		for _, e := range engines {
			require.NoError(t, e.Plan(frame))
		}
	}
	t.Logf("12 cycles, %d policy/budget engines: %s", len(engines), time.Since(started))
	for _, e := range engines {
		t.Logf("%s budget %d: plan %.3f s", e.result.Policy, e.result.Budget, e.result.PlanSeconds)
	}
}
