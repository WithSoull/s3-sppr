package sim

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func controlConfig() Config {
	c := DefaultConfig()
	c.HotBytes = 2 * model.MiB
	c.ColdBytes = 10 * model.MiB
	c.Utilization = .9
	c.Policies = []string{"frequency"}
	c.Budgets = []int64{model.MiB}
	c.Slots = 1
	return c
}
func controlCatalog(c Config) []model.Version {
	var v []model.Version
	for _, id := range []string{"a", "b"} {
		v = append(v, model.Version{VersionRef: model.VersionRef{Bucket: "b", ObjectKey: id, VersionID: id}, SizeBytes: model.MiB, CreatedAt: c.Generator.Start})
	}
	return v
}
func controlFrame() Frame {
	return Frame{At: 0, Features: make([]model.Features, 2), Forecasts: map[string][]float64{"frequency": {10, 0}}, ModelVersions: map[string]string{}}
}
func TestControlExample(t *testing.T) {
	// A copy of a: COLD read 40 ms + HOT write 10 ms, starts at 5 s.
	// A read arriving at 5.010 s waits until 5.050, then reads HOT in 10 ms:
	// latency 50 ms. The next read of b at 5.020 waits until 5.060,
	// reads COLD in 40 ms: latency 80 ms. Mean (50+80)/2 = 65 ms.
	c := controlConfig()
	reads := []Read{{int64(5010 * time.Millisecond), 0, model.MiB}, {int64(5020 * time.Millisecond), 1, model.MiB}}
	e, err := NewEngine(c, controlCatalog(c), reads, "frequency", model.MiB)
	require.NoError(t, err)
	require.NoError(t, e.Plan(controlFrame()))
	r, err := e.Finish()
	require.NoError(t, err)
	require.Equal(t, int64(1), r.Migrations)
	require.Equal(t, int64(model.MiB), r.CopyBytes)
	require.Equal(t, 65.0, r.Latency.MeanMs)
	require.Equal(t, 80.0, r.Latency.P95Ms)
	require.Equal(t, .5, r.HotShare)
	require.Equal(t, int64(2), r.Latency.Reads)
}
func TestDeterminismAndCacheEquivalence(t *testing.T) {
	c := controlConfig()
	c.Generator.Objects = 10
	c.Generator.ReadsPerSecond = 1.0 / 60
	a, err := Generate(context.Background(), c)
	require.NoError(t, err)
	b, err := Generate(context.Background(), c)
	require.NoError(t, err)
	require.Equal(t, a, b)
	f := controlFrame()
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(f))
	var cached Frame
	require.NoError(t, gob.NewDecoder(&encoded).Decode(&cached))
	var results []Result
	for _, frame := range []Frame{f, cached} {
		e, err := NewEngine(c, controlCatalog(c), []Read{{int64(6 * time.Second), 0, model.MiB}}, "frequency", model.MiB)
		require.NoError(t, err)
		require.NoError(t, e.Plan(frame))
		r, err := e.Finish()
		require.NoError(t, err)
		r.PlanSeconds = 0
		results = append(results, r)
	}
	require.Equal(t, results[0], results[1])
}
func TestInvariants(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Engine)
	}{
		{"capacity includes copy", func(e *Engine) { e.hot = 2 * model.MiB }},
		{"budget invalid forecast", func(e *Engine) { e.result.Budget = -1 }},
		{"duplicate action", func(e *Engine) {
			e.busy[0] = true
			e.pending = []action{{rec: model.Recommendation{VersionID: "a", TargetClass: model.StorageClassHot, SizeBytes: model.MiB}, object: 0, ready: int64(5 * time.Second)}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := controlConfig()
			e, err := NewEngine(c, controlCatalog(c), []Read{{int64(time.Second), 0, model.MiB}}, "frequency", model.MiB)
			require.NoError(t, err)
			tt.mutate(e)
			err = e.Plan(controlFrame())
			if err == nil {
				_, err = e.Finish()
			}
			require.Error(t, err)
		})
	}
}
func TestMinuteFeaturesAndCausalLabels(t *testing.T) {
	c := controlConfig()
	d := Demand{Catalog: controlCatalog(c), Minutes: make([][]minute, 10080)}
	d.Minutes[59] = []minute{{0, 2}}
	d.Minutes[60] = []minute{{1, 3}}
	rows, err := d.Snapshots(context.Background(), c, time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(2), rows[0].Features.Reads1h)
	require.Equal(t, int64(0), rows[1].Features.Reads1h)
	require.Equal(t, []int64{0, 3}, d.Labels(time.Hour))
}

func TestCopyKeepsFIFOPositionWhileAllSlotsBusy(t *testing.T) {
	c := controlConfig()
	c.ConfirmSeconds = 0
	// A COLD read takes the only slot at 0. A copy admitted at 10 ms must
	// start after that read at 40 ms, ahead of a read arriving at 20 ms.
	c.ConfirmSeconds = 0
	e, err := NewEngine(c, controlCatalog(c), []Read{{0, 1, model.MiB}, {int64(20 * time.Millisecond), 0, model.MiB}}, "frequency", model.MiB)
	require.NoError(t, err)
	require.NoError(t, e.Advance(int64(10*time.Millisecond)))
	frame := controlFrame()
	frame.At = int64(10 * time.Millisecond)
	require.NoError(t, e.Plan(frame))
	r, err := e.Finish()
	require.NoError(t, err)
	// First latency 40 ms. Second completes after copy (40+50) and HOT read
	// (+10), so 100-20=80 ms; mean 60 ms. Re-admitting the waiting copy on
	// every new read would let the second read overtake it.
	require.Equal(t, 60.0, r.Latency.MeanMs)
	require.Equal(t, .5, r.HotShare)
}

func TestRawLatenciesOutput(t *testing.T) {
	c := controlConfig()
	c.RawLatencies = true
	e, err := NewEngine(c, controlCatalog(c), []Read{{int64(5010 * time.Millisecond), 0, model.MiB}, {int64(5020 * time.Millisecond), 1, model.MiB}}, "frequency", model.MiB)
	require.NoError(t, err)
	require.NoError(t, e.Plan(controlFrame()))
	r, err := e.Finish()
	require.NoError(t, err)
	out := filepath.Join(t.TempDir(), "results")
	require.NoError(t, WriteResults(out, []Result{r}, "test"))
	require.Error(t, WriteResults(out, []Result{r}, "test"))
	f, err := os.Open(filepath.Join(out, "frequency-1048576-latencies.json.gz"))
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gz.Close()
	var latencies []int64
	require.NoError(t, json.NewDecoder(gz).Decode(&latencies))
	require.Equal(t, []int64{int64(50 * time.Millisecond), int64(80 * time.Millisecond)}, latencies)
}
