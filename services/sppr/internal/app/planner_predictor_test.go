package app

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
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
