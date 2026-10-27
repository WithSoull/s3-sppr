package admin_test

import (
	"context"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/admin"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestPages(t *testing.T) {
	p := model.Plan{PlanID: planID, Revision: 7, Status: model.PlanReady, MethodVersion: "ewma-1h-v1", Params: model.PlannerParams{Horizon: time.Hour}}
	recommendation := model.Recommendation{RecommendationID: recID, PlanID: planID, Selected: true, Key: "<script>alert(1)</script>", DependsOn: []string{commandID}}
	cases := []struct {
		name, path string
		build      func(*mocks.PlannerServiceMock, *mocks.MigrationServiceMock, *mocks.FormCommandsMock)
		contains   string
	}{
		{"status", "/admin", func(p *mocks.PlannerServiceMock, m *mocks.MigrationServiceMock, f *mocks.FormCommandsMock) {
			p.StatusMock.Return(model.PlannerStatus{MethodVersion: "ewma-1h-v1"}, nil)
		}, "ewma-1h-v1"},
		{"settings", "/admin/settings", func(p *mocks.PlannerServiceMock, m *mocks.MigrationServiceMock, f *mocks.FormCommandsMock) {
			p.GetSettingsMock.Return(model.PlannerSettings{Horizon: time.Hour}, nil)
			m.GetSettingsMock.Return(model.MigrationSettings{}, nil)
			m.GetBudgetMock.Return(model.BudgetStatus{}, nil)
			m.ListPinsMock.Return([]model.Pin{{Bucket: "b", Key: "k"}}, nil)
		}, "Закрепления"},
		{"latest recommendations with filter", "/admin/recommendations?selected=true&kind=promote&page_token=5", func(s *mocks.PlannerServiceMock, m *mocks.MigrationServiceMock, f *mocks.FormCommandsMock) {
			s.LatestPlanMock.Return(&p, nil)
			yes := true
			s.ListRecommendationsMock.Expect(minimock.AnyContext, model.RecommendationFilter{PlanID: planID, Selected: &yes, Kind: model.KindPromote, PageSize: 50, PageToken: "5"}).Return(model.RecommendationPage{Recommendations: []model.Recommendation{recommendation}, NextPageToken: "6"}, nil)
		}, "Следующая страница"},
		{"recommendation card", "/admin/recommendations/" + recID + "?plan_id=" + planID, func(s *mocks.PlannerServiceMock, m *mocks.MigrationServiceMock, f *mocks.FormCommandsMock) {
			s.GetPlanMock.Expect(minimock.AnyContext, planID).Return(p, nil)
			s.ListRecommendationsMock.Return(model.RecommendationPage{Recommendations: []model.Recommendation{recommendation}}, nil)
		}, "Ожидаемый эффект"},
		{"commands", "/admin/commands", func(p *mocks.PlannerServiceMock, m *mocks.MigrationServiceMock, f *mocks.FormCommandsMock) {
			p.ListCommandsMock.Return(model.CommandPage{Commands: []model.Command{{CommandRequest: model.CommandRequest{CommandID: commandID}}}}, nil)
			f.ListMock.Return(model.CommandPage{}, nil)
		}, commandID},
		{"action journal", "/admin/migrations/" + recID, func(p *mocks.PlannerServiceMock, m *mocks.MigrationServiceMock, f *mocks.FormCommandsMock) {
			m.GetActionMock.Expect(minimock.AnyContext, recID).Return(model.ActionDetails{Action: &model.Action{ActionSpec: model.ActionSpec{ActionID: recID, PlanID: planID}}, Events: []model.ActionEvent{{ToStatus: model.ActionSucceeded}}}, nil)
		}, "успешно завершено"},
		{"actions", "/admin/migrations?plan_id=" + planID, func(p *mocks.PlannerServiceMock, m *mocks.MigrationServiceMock, f *mocks.FormCommandsMock) {
			m.ListActionsMock.Expect(minimock.AnyContext, model.ActionFilter{PlanID: planID, PageSize: 50}).Return(model.ActionPage{Actions: []model.ActionView{{Action: &model.Action{ActionSpec: model.ActionSpec{ActionID: recID}}}}}, nil)
		}, recID},
		{"plan history", "/admin/plans", func(s *mocks.PlannerServiceMock, m *mocks.MigrationServiceMock, f *mocks.FormCommandsMock) {
			s.ListPlansMock.Return(model.PlanPage{Plans: []model.Plan{p}}, nil)
		}, "Ревизии"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			p := mocks.NewPlannerServiceMock(mc)
			m := mocks.NewMigrationServiceMock(mc)
			f := mocks.NewFormCommandsMock(mc)
			a := mocks.NewAuthorizerMock(mc)
			a.AuthorizeMock.Return(userID, nil)
			tt.build(p, m, f)
			h := admin.NewHandler(p, m, f, nil, a)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", tt.path, nil))
			require.Equal(t, 200, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), tt.contains)
			require.NotContains(t, w.Body.String(), "<script>")
			require.Contains(t, w.Body.String(), "<nav>")
			require.Contains(t, w.Body.String(), `action="/admin/logout"`)
			if strings.Contains(tt.path, "selected=true&kind=promote") {
				require.Contains(t, w.Body.String(), `value="promote" title="promote" selected>в HOT`)
			}
			for _, id := range regexp.MustCompile(`name="command_id" value="([^"]+)"`).FindAllStringSubmatch(w.Body.String(), -1) {
				assertUUID(t, id[1])
			}
		})
	}
}
func TestPlannerForms(t *testing.T) {
	cases := []struct {
		name, path string
		fields     url.Values
		want       model.CommandRequest
		result     model.Command
		status     int
		contains   string
	}{
		{name: "confirm checkboxes", path: "confirm", fields: url.Values{"plan_id": {planID}, "recommendation_id": {recID}}, want: model.CommandRequest{Type: model.CommandConfirm, PlanID: planID, RecommendationIDs: []string{recID}}, status: 200, contains: "Команда принята"},
		{name: "confirm all", path: "confirm", fields: url.Values{"plan_id": {planID}, "all": {"true"}}, want: model.CommandRequest{Type: model.CommandConfirm, PlanID: planID}, status: 200},
		{name: "reject", path: "reject", fields: url.Values{"plan_id": {planID}}, want: model.CommandRequest{Type: model.CommandReject, PlanID: planID}, status: 200},
		{name: "pause", path: "pause", want: model.CommandRequest{Type: model.CommandPause}, status: 200},
		{name: "resume", path: "resume", want: model.CommandRequest{Type: model.CommandResume}, status: 200},
		{name: "recompute", path: "recompute", want: model.CommandRequest{Type: model.CommandRecompute}, status: 200},
		{name: "cancel pending", path: "cancel_pending", fields: url.Values{"plan_id": {planID}}, want: model.CommandRequest{Type: model.CommandCancelPending, PlanID: planID}, status: 200},
		{name: "dependency refusal", path: "confirm", fields: url.Values{"plan_id": {planID}, "recommendation_id": {recID}}, want: model.CommandRequest{Type: model.CommandConfirm, PlanID: planID, RecommendationIDs: []string{recID}}, result: model.Command{Status: model.CommandRejected, Code: "DEPENDENCY_REQUIRED", Missing: []string{userID}}, status: 409, contains: "Добавьте перечисленные"},
		{name: "stale refusal", path: "reject", fields: url.Values{"plan_id": {planID}}, want: model.CommandRequest{Type: model.CommandReject, PlanID: planID}, result: model.Command{Status: model.CommandRejected, Code: "PLAN_STALE"}, status: 409, contains: "План устарел"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			p := mocks.NewPlannerServiceMock(mc)
			m := mocks.NewMigrationServiceMock(mc)
			a := mocks.NewAuthorizerMock(mc)
			a.AuthorizeMock.Return(userID, nil)
			tt.want.CommandID = commandID
			tt.want.Initiator = userID
			tt.result.CommandRequest = tt.want
			if tt.result.Status == "" {
				tt.result.Status = model.CommandAccepted
			}
			p.ExecuteMock.Expect(minimock.AnyContext, tt.want).Return(tt.result, nil)
			fields := tt.fields
			if fields == nil {
				fields = url.Values{}
			}
			fields.Set("command_id", commandID)
			h := admin.NewHandler(p, m, nil, nil, a)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, post("/admin/commands/"+tt.path, fields))
			require.Equal(t, tt.status, w.Code, w.Body.String())
			if tt.contains != "" {
				require.Contains(t, w.Body.String(), tt.contains)
			}
			if tt.result.Code == "DEPENDENCY_REQUIRED" {
				require.Contains(t, w.Body.String(), userID)
			}
		})
	}
}
func TestExecutorForms(t *testing.T) {
	cases := []struct {
		name, operation string
		fields          url.Values
		build           func(*mocks.MigrationServiceMock)
	}{
		{"budget", "budget", url.Values{"bytes": {"1024"}, "window_seconds": {"60"}}, func(m *mocks.MigrationServiceMock) {
			m.SetBudgetMock.Expect(minimock.AnyContext, 1024, time.Minute).Return(nil)
		}},
		{"interval", "interval", url.Values{"min_interval_seconds": {"60"}}, func(m *mocks.MigrationServiceMock) {
			m.SetMinIntervalMock.Expect(minimock.AnyContext, time.Minute).Return(nil)
		}},
		{"add pin uses authorized author", "pin-add", url.Values{"bucket": {"b"}, "key": {"a/b"}, "reason": {"test"}, "author": {"forged"}}, func(m *mocks.MigrationServiceMock) {
			m.AddPinMock.Expect(minimock.AnyContext, model.Pin{Bucket: "b", Key: "a/b", Reason: "test", Author: userID}).Return(model.Pin{}, nil)
		}},
		{"delete pin", "pin-delete", url.Values{"bucket": {"b"}, "key": {"a/b"}}, func(m *mocks.MigrationServiceMock) {
			m.DeletePinMock.Expect(minimock.AnyContext, "b", "a/b").Return(nil)
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			p := mocks.NewPlannerServiceMock(mc)
			m := mocks.NewMigrationServiceMock(mc)
			f := mocks.NewFormCommandsMock(mc)
			a := mocks.NewAuthorizerMock(mc)
			a.AuthorizeMock.Return(userID, nil)
			tt.build(m)
			tt.fields.Set("command_id", commandID)
			f.ExecuteMock.Set(func(ctx context.Context, req model.CommandRequest, payload string, apply func(context.Context) error) (model.Command, error) {
				require.Equal(t, commandID, req.CommandID)
				require.Equal(t, userID, req.Initiator)
				require.Equal(t, tt.operation+"?"+tt.fields.Encode(), payload)
				require.NoError(t, apply(ctx))
				return model.Command{CommandRequest: req, Status: model.CommandAccepted}, nil
			})
			h := admin.NewHandler(p, m, f, nil, a)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, post("/admin/settings/"+tt.operation, tt.fields))
			require.Equal(t, 200, w.Code, w.Body.String())
		})
	}
}
func TestInvalidForms(t *testing.T) {
	cases := []struct {
		name, path, origin, referer string
		fields                      url.Values
		want                        int
	}{
		{name: "foreign origin", path: "/admin/commands/recompute", origin: "https://evil.test", want: 403},
		{name: "foreign referer", path: "/admin/settings/budget", referer: "https://evil.test/form", want: 403},
		{name: "empty selection cannot confirm whole plan", path: "/admin/commands/confirm", fields: url.Values{"command_id": {commandID}, "plan_id": {planID}}, want: 400},
		{name: "bad duration", path: "/admin/settings/interval", fields: url.Values{"command_id": {commandID}, "min_interval_seconds": {"999999999999999999"}}, want: 400},
		{name: "bad id", path: "/admin/settings/budget", fields: url.Values{"command_id": {"bad"}}, want: 400},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			a := mocks.NewAuthorizerMock(mc)
			if tt.origin == "" && tt.referer == "" {
				a.AuthorizeMock.Return(userID, nil)
			}
			h := admin.NewHandler(mocks.NewPlannerServiceMock(mc), mocks.NewMigrationServiceMock(mc), mocks.NewFormCommandsMock(mc), nil, a)
			r := post(tt.path, tt.fields)
			r.Header.Set("Origin", tt.origin)
			r.Header.Set("Referer", tt.referer)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			require.Equal(t, tt.want, w.Code, w.Body.String())
		})
	}
}
func TestPlannerSettingsForm(t *testing.T) {
	mc := minimock.NewController(t)
	p := mocks.NewPlannerServiceMock(mc)
	a := mocks.NewAuthorizerMock(mc)
	a.AuthorizeMock.Return(userID, nil)
	fields := url.Values{"command_id": {commandID}, "horizon_seconds": {"3600"}, "hot_fixed_ms": {"1"}, "hot_mib_per_ms": {"0.5"}, "cold_fixed_ms": {"20"}, "cold_mib_per_ms": {"0.05"}, "penalty_ms_per_mib": {"2"}, "min_gain_ms": {"100"}, "hot_utilization": {"0.9"}, "cold_utilization": {"0.8"}}
	want := model.PlannerSettings{Horizon: time.Hour, HotFixedMs: 1, HotMiBPerMs: 0.5, ColdFixedMs: 20, ColdMiBPerMs: 0.05, PenaltyMsPerMiB: 2, MinGainMs: 100, HotUtilization: 0.9, ColdUtilization: 0.8}
	p.ExecuteMock.Expect(minimock.AnyContext, model.CommandRequest{CommandID: commandID, Initiator: userID, Type: model.CommandSettings, Settings: &want}).Return(model.Command{Status: model.CommandAccepted}, nil)
	h := admin.NewHandler(p, mocks.NewMigrationServiceMock(mc), nil, nil, a)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, post("/admin/settings/planner", fields))
	require.Equal(t, 200, w.Code, w.Body.String())
}
func TestStoredRefusalPage(t *testing.T) {
	mc := minimock.NewController(t)
	p := mocks.NewPlannerServiceMock(mc)
	a := mocks.NewAuthorizerMock(mc)
	a.AuthorizeMock.Return(userID, nil)
	p.GetCommandMock.Expect(minimock.AnyContext, commandID).Return(model.Command{CommandRequest: model.CommandRequest{CommandID: commandID}, Status: model.CommandRejected, Code: "PLAN_STALE"}, nil)
	h := admin.NewHandler(p, mocks.NewMigrationServiceMock(mc), nil, nil, a)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/admin/commands/"+commandID, nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "План устарел")
}
func TestRefusalHelp(t *testing.T) {
	for _, code := range strings.Fields("PLAN_STALE DEPENDENCY_REQUIRED HISTORY_INCOMPLETE ACCESS_DENIED BUDGET_EXHAUSTED CAPACITY_LIMIT MODEL_UNAVAILABLE") {
		t.Run(code, func(t *testing.T) {
			help := admin.Help(code)
			require.NotEmpty(t, help)
			require.NotEqual(t, code, help)
		})
	}
}

func TestRecommendationPresentation(t *testing.T) {
	mc := minimock.NewController(t)
	p := mocks.NewPlannerServiceMock(mc)
	a := mocks.NewAuthorizerMock(mc)
	a.AuthorizeMock.Return(userID, nil)
	p.GetPlanMock.Expect(minimock.AnyContext, planID).Return(model.Plan{PlanID: planID}, nil)
	p.ListRecommendationsMock.Return(model.RecommendationPage{Recommendations: []model.Recommendation{{
		RecommendationID: recID, PlanID: planID, Selected: true, Kind: model.KindPromote, SizeBytes: 65536,
		Explanation: model.Explanation{Forecast: model.ForecastExplanation{HorizonSeconds: 3600, ExpectedReads: 12, ReadFraction: 1, Percentile: 66.6666}, Effect: model.EffectExplanation{GainMs: 1491.26}},
	}}}, nil)
	h := admin.NewHandler(p, mocks.NewMigrationServiceMock(mc), nil, nil, a)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/admin/recommendations/"+recID+"?plan_id="+planID, nil))
	require.Equal(t, 200, w.Code, w.Body.String())
	for _, s := range []string{`title="65536 байт">64 КиБ`, `title="promote">в HOT`, "1 491,3 мс", "12,0 / 100 %", "66,7", "1 ч", `да / <span title="">—</span>`} {
		require.Contains(t, w.Body.String(), s)
	}
}
