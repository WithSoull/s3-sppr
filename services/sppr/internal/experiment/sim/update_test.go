package sim

import (
	"bufio"
	"context"
	"encoding/gob"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/baseline"
	"github.com/stretchr/testify/require"
)

func TestModelSwitchWaitsForMeasuredTraining(t *testing.T) {
	dir := t.TempDir()
	c := DefaultConfig()
	c.Policies = []string{"lgbm"}
	for _, kind := range []struct{ name, feature string }{{"age", "age_seconds"}, {"recency", "last_read_seconds"}} {
		m, err := baseline.Fit(kind.feature, []baseline.Sample{{X: 0, Reads: 1}})
		require.NoError(t, err)
		require.NoError(t, m.Save(filepath.Join(dir, kind.name+"-baseline-map.json")))
	}
	fixture := filepath.Join("..", "..", "service", "predictor", "lgbm", "testdata")
	for _, name := range []string{"initial", "updated"} {
		target := filepath.Join(dir, name)
		require.NoError(t, os.Mkdir(target, 0755))
		for _, file := range []string{"classifier.txt", "regressor.txt", "parity.json", "manifest.json"} {
			b, err := os.ReadFile(filepath.Join(fixture, file))
			require.NoError(t, err)
			if file == "manifest.json" {
				var m map[string]any
				require.NoError(t, json.Unmarshal(b, &m))
				m["model_version"] = name
				b, err = json.Marshal(m)
				require.NoError(t, err)
			}
			require.NoError(t, os.WriteFile(filepath.Join(target, file), b, 0644))
		}
	}
	f, err := os.Create(filepath.Join(dir, "features.gob"))
	require.NoError(t, err)
	enc := gob.NewEncoder(f)
	for _, at := range []time.Duration{96 * time.Hour, 96*time.Hour + 5*time.Minute, 96*time.Hour + 10*time.Minute} {
		require.NoError(t, enc.Encode(Frame{At: int64(at), Features: []model.Features{{EWMA1h: 1}}}))
	}
	require.NoError(t, f.Close())
	training := []Training{{Cutoff: int64(48 * time.Hour), Activation: int64(48*time.Hour + time.Second), Dir: "initial"}, {Cutoff: int64(96 * time.Hour), Activation: int64(96*time.Hour + 301*time.Second), Dir: "updated"}}
	catalog := []model.Version{{VersionRef: model.VersionRef{Bucket: "b", ObjectKey: "k", VersionID: "v"}, SizeBytes: model.MiB, CreatedAt: c.Generator.Start}}
	require.NoError(t, Forecast(context.Background(), c, dir, catalog, training))
	f, err = os.Open(filepath.Join(dir, "forecasts.gob"))
	require.NoError(t, err)
	defer f.Close()
	dec := gob.NewDecoder(bufio.NewReader(f))
	for _, want := range []string{"initial", "initial", "updated"} {
		var frame Frame
		require.NoError(t, dec.Decode(&frame))
		require.Equal(t, want, frame.ModelVersions["lgbm"])
	}
}
