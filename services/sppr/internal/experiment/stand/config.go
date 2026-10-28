// Package stand owns disposable Compose runs. It never targets an existing stand.
package stand

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/loadgen"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/baseline"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
)

const Version = "placement-stand-v1"

var OverheadPolicies = []string{"no-extension", "sppr-no-training", "sppr-update"}

type Config struct {
	Load                 loadgen.Config `json:"load"`
	ComposeFile          string         `json:"compose_file"`
	ModelDir             string         `json:"model_dir"`
	AgeMap               string         `json:"age_map"`
	RecencyMap           string         `json:"recency_map"`
	Overhead             bool           `json:"overhead"`
	TrainingData         string         `json:"training_data"`
	TrainingEverySeconds float64        `json:"training_every_seconds"`
	StatsSeconds         float64        `json:"stats_seconds"`
	StartupSeconds       float64        `json:"startup_seconds"`
	RawLatencies         bool           `json:"raw_latencies"`
}

func DefaultConfig() Config {
	return Config{Load: loadgen.Config{Generator: sim.DefaultConfig().Generator, Schedule: "synthetic", DurationSeconds: 21600, TimeCompression: 1, Slots: 8, RequestTimeoutSeconds: 300}, ComposeFile: "docker-compose.yml", StatsSeconds: 5, StartupSeconds: 600, TrainingEverySeconds: 300}
}
func (c Config) Validate(sc sim.Config) error {
	if err := c.Load.Validate(); err != nil {
		return err
	}
	for _, v := range []float64{c.StatsSeconds, c.StartupSeconds, c.TrainingEverySeconds} {
		if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("invalid stand polling/training interval")
		}
	}
	if c.ComposeFile == "" {
		return fmt.Errorf("compose_file required")
	}
	if sc.ReadFraction != 1 || len(sc.Sizes) > 0 {
		return fmt.Errorf("stand requires full reads and one object size in stand.load.generator")
	}
	if sc.MigrationParallelism != 1 {
		return fmt.Errorf("stand requires migration_parallelism=1")
	}
	if c.Overhead {
		if len(sc.Budgets) != 1 {
			return fmt.Errorf("overhead requires exactly one budget")
		}
		if c.TrainingData == "" {
			return fmt.Errorf("overhead requires training_data (train-only dataset)")
		}
		if c.Load.Schedule != "constant" {
			return fmt.Errorf("overhead requires constant schedule")
		}
	}
	return nil
}
func expand(p, scenario string, seed uint64) string {
	return strings.ReplaceAll(strings.ReplaceAll(p, "{scenario}", scenario), "{seed}", fmt.Sprint(seed))
}
func (c Config) Artifacts(sc sim.Config, scenario string, seed uint64) (map[string]string, error) {
	out := map[string]string{}
	policies := sc.Policies
	if c.Overhead {
		policies = nil
	}
	for _, p := range policies {
		switch p {
		case "lgbm":
			path := expand(c.ModelDir, scenario, seed)
			if _, err := lgbm.Load(path, time.Hour); err != nil {
				return nil, fmt.Errorf("stand model: %w", err)
			}
			out["model"] = path
		case "age", "recency":
			path := c.AgeMap
			if p == "recency" {
				path = c.RecencyMap
			}
			path = expand(path, scenario, seed)
			m, err := baseline.Load(path)
			if err != nil {
				return nil, err
			}
			if _, err = baseline.New(p, m); err != nil {
				return nil, err
			}
			out[p] = path
		}
	}
	if c.Overhead {
		path := expand(c.TrainingData, scenario, seed)
		raw, err := os.ReadFile(filepath.Join(path, "manifest.json"))
		if err != nil {
			return nil, err
		}
		var manifest struct {
			Train      struct{ Start, End time.Time }
			Validation struct{ Start, End time.Time }
			Test       struct{ Start, End time.Time }
		}
		if err = json.Unmarshal(raw, &manifest); err != nil {
			return nil, err
		}
		if !manifest.Train.End.After(manifest.Train.Start) || !manifest.Validation.Start.Equal(manifest.Validation.End) || !manifest.Test.Start.Equal(manifest.Test.End) {
			return nil, fmt.Errorf("overhead training_data must contain train only (empty validation/test intervals)")
		}
		out["training"] = path
	}
	for key, path := range out {
		real, err := filepath.EvalSymlinks(path)
		if err != nil {
			return nil, err
		}
		out[key] = real
	}
	return out, nil
}
