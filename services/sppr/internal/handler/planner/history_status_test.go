package planner_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestHistoryStatusDTO(t *testing.T) {
	mc := minimock.NewController(t)
	p := mocks.NewPlannerServiceMock(mc)
	c := model.Coverage{Known: true, Start: created, Watermark: created, OpenStreams: 2, ClosedStreams: 3, LostStreams: 1, Gaps24h: 4}
	objects := []model.ObjectHistory{{VersionRef: model.VersionRef{Bucket: "b", ObjectKey: "k", VersionID: "v"}, State: "история неполна", Quality: model.DataQuality{Reason: "gap"}}}
	p.StatusMock.Return(model.PlannerStatus{History: c, HistoryLagSeconds: 61, Objects: objects}, nil)
	w := httptest.NewRecorder()
	planner.NewHandler(p).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/planner/status", nil))
	require.Equal(t, 200, w.Code)
	var got struct {
		History model.Coverage        `json:"history"`
		Lag     float64               `json:"history_lag_seconds"`
		Objects []model.ObjectHistory `json:"objects"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, c, got.History)
	require.Equal(t, 61.0, got.Lag)
	require.Equal(t, objects, got.Objects)
}
