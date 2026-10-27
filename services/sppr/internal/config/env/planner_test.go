package env

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlannerPredictorConfig(t *testing.T) {
	for _, tt := range []struct{ name, kind, dir, want, wantErr string }{
		{name: "default", want: "ewma"},
		{name: "explicit EWMA", kind: "ewma", want: "ewma"},
		{name: "ML", kind: "lgbm", dir: "/models/run", want: "lgbm"},
		{name: "ML missing directory is handled by fallback", kind: "lgbm", want: "lgbm"},
		{name: "unknown predictor", kind: "typo", wantErr: "PLANNER_PREDICTOR"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PLANNER_PREDICTOR", tt.kind)
			t.Setenv("PLANNER_MODEL_DIR", tt.dir)
			got, err := NewPlannerConfig()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got.Predictor())
			require.Equal(t, tt.dir, got.ModelDir())
		})
	}
}
