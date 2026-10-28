package planner_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestRetrainRoute(t *testing.T) {
	mc := minimock.NewController(t)
	s := mocks.NewPlannerServiceMock(mc)
	req := model.CommandRequest{CommandID: cmdID, Initiator: "alice", Type: model.CommandRetrain}
	s.ExecuteMock.Expect(minimock.AnyContext, req).Return(model.Command{CommandRequest: req, Status: model.CommandAccepted}, nil)
	h := access.Middleware(user("alice"), planner.NewHandler(s))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/planner/model/retrain", strings.NewReader(`{"command_id":"`+cmdID+`"}`)))
	require.Equal(t, 202, w.Code)
}
func TestLifecycleStatusIsAdditive(t *testing.T) {
	mc := minimock.NewController(t)
	s := mocks.NewPlannerServiceMock(mc)
	s.StatusMock.Return(model.PlannerStatus{MethodVersion: "ewma-1h-v1", ModelLifecycle: &model.ModelLifecycleStatus{FallbackReason: "model_stale", MaxAgeSeconds: 259200, Progress: "training", Training: true}}, nil)
	h := access.Middleware(user("alice"), planner.NewHandler(s))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/planner/status", nil))
	require.Equal(t, 200, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "ewma-1h-v1", body["method_version"])
	require.Contains(t, body, "history_quality")
	lifecycle := body["model_lifecycle"].(map[string]any)
	require.Equal(t, "model_stale", lifecycle["fallback_reason"])
	require.Equal(t, true, lifecycle["training"])
}
