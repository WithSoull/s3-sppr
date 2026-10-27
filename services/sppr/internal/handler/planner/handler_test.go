package planner_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

const (
	planID = "11111111-1111-4111-8111-111111111111"
	cmdID  = "22222222-2222-4222-8222-222222222222"
	recID  = "33333333-3333-4333-8333-333333333333"
)

type user string

func (u user) Authorize(*http.Request) (string, error) {
	if u == "" {
		return "", fmt.Errorf("%w: not an administrator", model.ErrAccessDenied)
	}
	return string(u), nil
}

var created = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func TestHandler(t *testing.T) {
	logger.SetNopLogger()
	confirm := model.CommandRequest{CommandID: cmdID, Initiator: "alice", Type: model.CommandConfirm, PlanID: planID, RecommendationIDs: []string{recID}}
	cases := []struct {
		name       string
		user       user
		method     string
		target     string
		body       string
		build      func(*mocks.PlannerServiceMock)
		wantStatus int
		wantBody   string
	}{
		{
			name: "accepted confirm acts for the authorized user", user: "alice",
			method: http.MethodPost, target: "/api/planner/plans/" + planID + "/confirm",
			body: `{"command_id":"` + cmdID + `","recommendation_ids":["` + recID + `"]}`,
			build: func(s *mocks.PlannerServiceMock) {
				s.ExecuteMock.Expect(minimock.AnyContext, confirm).Return(model.Command{
					CommandRequest: confirm, Status: model.CommandAccepted, Actions: []string{recID}, CreatedAt: created,
				}, nil)
			},
			wantStatus: http.StatusAccepted,
			wantBody: `{"command_id":"` + cmdID + `","type":"confirm","initiator":"alice","plan_id":"` + planID +
				`","recommendation_ids":["` + recID + `"],"status":"accepted","actions":["` + recID + `"],"created_at":"2026-10-28T12:00:00Z"}`,
		},
		{
			name: "refused confirm answers 409 with the code and what is missing", user: "alice",
			method: http.MethodPost, target: "/api/planner/plans/" + planID + "/confirm",
			body: `{"command_id":"` + cmdID + `","recommendation_ids":["` + recID + `"]}`,
			build: func(s *mocks.PlannerServiceMock) {
				s.ExecuteMock.Return(model.Command{CommandRequest: confirm, Status: model.CommandRejected,
					Code: "DEPENDENCY_REQUIRED", Missing: []string{planID}, CreatedAt: created}, nil)
			},
			wantStatus: http.StatusConflict,
			wantBody: `{"command_id":"` + cmdID + `","type":"confirm","initiator":"alice","plan_id":"` + planID +
				`","recommendation_ids":["` + recID + `"],"status":"rejected","code":"DEPENDENCY_REQUIRED","missing":["` + planID + `"],"created_at":"2026-10-28T12:00:00Z"}`,
		},
		{
			name: "without administrator rights nothing is called", user: "",
			method: http.MethodPost, target: "/api/planner/plans/" + planID + "/confirm", body: `{"command_id":"` + cmdID + `"}`,
			build:      func(*mocks.PlannerServiceMock) {},
			wantStatus: http.StatusForbidden,
			wantBody:   `{"code":"ACCESS_DENIED","error":"ACCESS_DENIED: not an administrator"}`,
		},
		{
			name: "reused command_id conflicts", user: "alice",
			method: http.MethodPost, target: "/api/planner/commands/pause", body: `{"command_id":"` + cmdID + `"}`,
			build: func(s *mocks.PlannerServiceMock) {
				s.ExecuteMock.Return(model.Command{}, fmt.Errorf("%w: command %s", model.ErrCommandConflict, cmdID))
			},
			wantStatus: http.StatusConflict,
			wantBody:   `{"error":"command conflicts with an accepted one: command ` + cmdID + `"}`,
		},
		{
			name: "unknown field is a client error", user: "alice",
			method: http.MethodPost, target: "/api/planner/plans/recompute", body: `{"command_id":"` + cmdID + `","x":1}`,
			build:      func(*mocks.PlannerServiceMock) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "cancel takes action IDs from the body", user: "alice",
			method: http.MethodPost, target: "/api/planner/commands/cancel-pending",
			body: `{"command_id":"` + cmdID + `","action_ids":["` + recID + `"]}`,
			build: func(s *mocks.PlannerServiceMock) {
				req := model.CommandRequest{CommandID: cmdID, Initiator: "alice", Type: model.CommandCancelPending, RecommendationIDs: []string{recID}}
				s.ExecuteMock.Expect(minimock.AnyContext, req).Return(model.Command{CommandRequest: req, Status: model.CommandAccepted, CreatedAt: created}, nil)
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "unknown plan is 404", user: "alice",
			method: http.MethodGet, target: "/api/planner/plans/" + planID,
			build: func(s *mocks.PlannerServiceMock) {
				s.GetPlanMock.Return(model.Plan{}, model.ErrPlanNotFound)
			},
			wantStatus: http.StatusNotFound,
			wantBody:   `{"error":"plan not found"}`,
		},
		{
			name: "recommendations filter and page", user: "alice",
			method: http.MethodGet, target: "/api/planner/plans/" + planID + "/recommendations?selected=true&kind=promote&page_size=2&page_token=4",
			build: func(s *mocks.PlannerServiceMock) {
				selected := true
				s.ListRecommendationsMock.Expect(minimock.AnyContext, model.RecommendationFilter{
					PlanID: planID, Selected: &selected, Kind: model.KindPromote, PageSize: 2, PageToken: "4",
				}).Return(model.RecommendationPage{NextPageToken: "6"}, nil)
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"next_page_token":"6","recommendations":[]}`,
		},
		{
			name: "settings change is a journaled command", user: "alice",
			method: http.MethodPut, target: "/api/planner/settings",
			body: `{"command_id":"` + cmdID + `","horizon_seconds":3600,"hot_fixed_ms":1,"hot_mib_per_ms":1,"cold_fixed_ms":20,"cold_mib_per_ms":0.1,"penalty_ms_per_mib":2,"min_gain_ms":0,"hot_utilization":0.9,"cold_utilization":0.9}`,
			build: func(s *mocks.PlannerServiceMock) {
				settings := model.PlannerSettings{Horizon: time.Hour, HotFixedMs: 1, HotMiBPerMs: 1, ColdFixedMs: 20, ColdMiBPerMs: 0.1,
					PenaltyMsPerMiB: 2, HotUtilization: 0.9, ColdUtilization: 0.9}
				req := model.CommandRequest{CommandID: cmdID, Initiator: "alice", Type: model.CommandSettings, Settings: &settings}
				s.ExecuteMock.Expect(minimock.AnyContext, req).Return(model.Command{CommandRequest: req, Status: model.CommandAccepted, CreatedAt: created}, nil)
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name: "internal errors are not leaked", user: "alice",
			method: http.MethodGet, target: "/api/planner/plans",
			build: func(s *mocks.PlannerServiceMock) {
				s.ListPlansMock.Return(model.PlanPage{}, errors.New("pg: connection refused"))
			},
			wantStatus: http.StatusInternalServerError,
			wantBody:   `{"error":"internal error"}`,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			s := mocks.NewPlannerServiceMock(mc)
			tt.build(s)
			h := access.Middleware(tt.user, planner.NewHandler(s))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.target, strings.NewReader(tt.body)))
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			if tt.wantBody != "" {
				require.JSONEq(t, tt.wantBody, rec.Body.String())
			}
		})
	}
}
