package app

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/baseline"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
	"github.com/stretchr/testify/require"
)

func TestPlannerPredictor(t *testing.T) {
	fixture := filepath.Join("..", "service", "predictor", "lgbm", "testdata")
	for _, tt := range []struct {
		name, kind, dir         string
		storedHorizon           time.Duration
		method, version, reason string
	}{
		{"default EWMA", "ewma", "/ignored", time.Hour, ewma.MethodVersion, "", ""},
		{"ML fixture", "lgbm", fixture, time.Hour, lgbm.MethodVersion, "spike-fixture-v1", ""},
		{"missing model", "lgbm", "/nonexistent-sppr-model", time.Hour, ewma.MethodVersion, "", "manifest.json"},
		{"no directory", "lgbm", "", time.Hour, ewma.MethodVersion, "", "PLANNER_MODEL_DIR is empty"},
		{"stored horizon differs", "lgbm", fixture, 2 * time.Hour, ewma.MethodVersion, "", "stored planner horizon"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PLANNER_PREDICTOR", tt.kind)
			t.Setenv("PLANNER_MODEL_DIR", tt.dir)
			t.Setenv("PLANNER_HORIZON", "1h")
			cfg, err := env.NewPlannerConfig()
			require.NoError(t, err)
			got, reason := plannerPredictor(cfg, tt.storedHorizon)
			require.Equal(t, tt.method, got.MethodVersion())
			require.Equal(t, tt.version, got.ModelVersion())
			if tt.reason == "" {
				require.Empty(t, reason)
			} else {
				require.Contains(t, reason, tt.reason)
			}
			forecast, err := got.Predict(model.FeatureSnapshot{At: time.Now(), RulesVersion: model.FeatureRulesVersion, Features: model.Features{EWMA1h: 12}}, time.Hour)
			require.NoError(t, err)
			if tt.method == ewma.MethodVersion {
				require.Equal(t, 12.0, forecast.ExpectedReads)
				require.Nil(t, forecast.Hurdle)
			}
		})
	}
}

func TestPlannerBaselineChoice(t *testing.T) {
	for _, tt := range []struct {
		name, kind, feature string
		bad                 bool
	}{
		{"frequency", "frequency", "", false}, {"age", "age", "age_seconds", false}, {"recency", "recency", "last_read_seconds", false}, {"wrong feature falls back", "recency", "age_seconds", true}, {"missing map falls back", "age", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "baseline-map.json")
			if tt.feature != "" {
				m, err := baseline.Fit(tt.feature, []baseline.Sample{{X: 0, Reads: 1}, {X: 1, Reads: 2}})
				require.NoError(t, err)
				require.NoError(t, m.Save(path))
			}
			t.Setenv("PLANNER_PREDICTOR", tt.kind)
			t.Setenv("PLANNER_BASELINE_MAP", path)
			cfg, err := env.NewPlannerConfig()
			require.NoError(t, err)
			p, reason := plannerPredictor(cfg, time.Hour)
			if tt.bad {
				require.NotEmpty(t, reason)
				require.Equal(t, ewma.MethodVersion, p.MethodVersion())
			} else {
				require.Empty(t, reason)
				require.Equal(t, "baseline-"+tt.kind+"-v1", p.MethodVersion())
			}
		})
	}
}
