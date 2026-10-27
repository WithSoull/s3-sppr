package planner_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestStatusPredictorIdentity(t *testing.T) {
	for _, tt := range []struct{ name, method, version, reason string }{
		{"ML", "lgbm-hurdle-v1", "spike-fixture-v1", ""},
		{"fallback", "ewma-1h-v1", "", "parity.json: expected_reads parity mismatch"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			s := mocks.NewPlannerServiceMock(mc)
			s.StatusMock.Return(model.PlannerStatus{MethodVersion: tt.method, ModelVersion: tt.version, FallbackReason: tt.reason, LatestPlan: &model.Plan{MethodVersion: tt.method, ModelVersion: tt.version}}, nil)
			h := access.Middleware(user("alice"), planner.NewHandler(s))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/api/planner/status", nil))
			require.Equal(t, 200, w.Code, w.Body.String())
			var got map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			require.Equal(t, tt.method, got["method_version"])
			require.Equal(t, tt.version, got["model_version"])
			require.Equal(t, tt.reason, got["fallback_reason"])
			plan := got["latest_plan"].(map[string]any)
			require.Equal(t, tt.method, plan["method_version"])
			require.Equal(t, tt.version, plan["model_version"])
		})
	}
}
