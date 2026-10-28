package app

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/baseline"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/stretchr/testify/require"
)

func TestFixedPredictorEWMA(t *testing.T) {
	t.Setenv("PLANNER_PREDICTOR", "ewma")
	cfg, err := env.NewPlannerConfig()
	require.NoError(t, err)
	got, reason := fixedPredictor(cfg)
	require.Empty(t, reason)
	require.Equal(t, ewma.MethodVersion, got.MethodVersion())
	forecast, err := got.Predict(model.FeatureSnapshot{At: time.Now(), RulesVersion: model.FeatureRulesVersion, Features: model.Features{EWMA1h: 12}}, time.Hour)
	require.NoError(t, err)
	require.Equal(t, 12.0, forecast.ExpectedReads)
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
			p, reason := fixedPredictor(cfg)
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
