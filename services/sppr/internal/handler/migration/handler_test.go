package migration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/migration"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

const (
	planID   = "11111111-1111-4111-8111-111111111111"
	actionID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
)

func TestHandler(t *testing.T) {
	logger.SetNopLogger()
	submitBody := `{"plan_id":"` + planID + `","actions":[{"action_id":"` + actionID + `","version_id":"v","expected_generation":1,"source_class":"COLD","target_class":"HOT","size_bytes":10,"depends_on":[]}]}`
	cases := []struct {
		name       string
		method     string
		target     string
		body       string
		build      func(*mocks.MigrationServiceMock)
		wantStatus int
		wantBody   string
	}{
		{
			name: "submit accepts", method: http.MethodPost, target: "/api/migration/actions", body: submitBody,
			build: func(s *mocks.MigrationServiceMock) {
				s.SubmitActionsMock.Expect(minimock.AnyContext, planID, []model.ActionSpec{{
					ActionID: actionID, VersionID: "v", ExpectedGeneration: 1, SourceClass: model.StorageClassCold,
					TargetClass: model.StorageClassHot, SizeBytes: 10, DependsOn: []string{},
				}}).Return(model.SubmitResult{Accepted: []string{actionID}, Existing: []string{}}, nil)
			},
			wantStatus: http.StatusAccepted, wantBody: `{"accepted":["` + actionID + `"],"existing":[]}`,
		},
		{
			name: "submit conflict", method: http.MethodPost, target: "/api/migration/actions", body: submitBody,
			build: func(s *mocks.MigrationServiceMock) {
				s.SubmitActionsMock.Return(model.SubmitResult{}, model.ErrActionConflict)
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "submit validation", method: http.MethodPost, target: "/api/migration/actions", body: submitBody,
			build: func(s *mocks.MigrationServiceMock) {
				s.SubmitActionsMock.Return(model.SubmitResult{}, model.ErrInvalidArgument)
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown field", method: http.MethodPost, target: "/api/migration/actions",
			body: `{"plan_id":"` + planID + `","action":[]}`, wantStatus: http.StatusBadRequest,
		},
		{
			name: "malformed JSON", method: http.MethodPost, target: "/api/migration/actions",
			body: `{`, wantStatus: http.StatusBadRequest,
		},
		{
			name: "bad page_size", method: http.MethodGet, target: "/api/migration/actions?page_size=x",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "list passes filters", method: http.MethodGet, target: "/api/migration/actions?plan_id=" + planID + "&status=pending&page_size=5&page_token=" + actionID,
			build: func(s *mocks.MigrationServiceMock) {
				s.ListActionsMock.Expect(minimock.AnyContext, model.ActionFilter{PlanID: planID, Status: model.ActionPending, PageSize: 5, PageToken: actionID}).
					Return(model.ActionPage{Actions: []model.ActionView{{
						Action:    &model.Action{ActionSpec: model.ActionSpec{ActionID: actionID}, Status: model.ActionPending},
						BlockedBy: model.BlockedByBudget,
					}}}, nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "unknown action", method: http.MethodGet, target: "/api/migration/actions/" + actionID,
			build: func(s *mocks.MigrationServiceMock) {
				s.GetActionMock.Expect(minimock.AnyContext, actionID).Return(model.ActionDetails{}, model.ErrActionNotFound)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "internal error hides details", method: http.MethodGet, target: "/api/migration/budget",
			build: func(s *mocks.MigrationServiceMock) {
				s.GetBudgetMock.Return(model.BudgetStatus{}, errors.New("password=secret"))
			},
			wantStatus: http.StatusInternalServerError, wantBody: `{"error":"internal error"}`,
		},
		{
			name: "budget update", method: http.MethodPut, target: "/api/migration/budget", body: `{"bytes":1024,"window_seconds":60}`,
			build: func(s *mocks.MigrationServiceMock) {
				s.SetBudgetMock.Expect(minimock.AnyContext, 1024, time.Minute).Return(nil)
				s.GetBudgetMock.Return(model.BudgetStatus{Bytes: 1024, Window: time.Minute, Spent: 24, Remaining: 1000}, nil)
			},
			wantStatus: http.StatusOK, wantBody: `{"bytes":1024,"window_seconds":60,"spent":24,"remaining":1000}`,
		},
		{
			name: "budget spent is read-only", method: http.MethodPut, target: "/api/migration/budget",
			body: `{"bytes":1024,"window_seconds":60,"spent":0}`, wantStatus: http.StatusBadRequest,
		},
		{
			name: "settings paused is read-only", method: http.MethodPut, target: "/api/migration/settings",
			body: `{"min_interval_seconds":60,"paused":false}`, wantStatus: http.StatusBadRequest,
		},
		{
			name: "cancel", method: http.MethodPost, target: "/api/migration/actions/cancel", body: `{"plan_id":"` + planID + `"}`,
			build: func(s *mocks.MigrationServiceMock) {
				s.CancelActionsMock.Expect(minimock.AnyContext, model.CancelRequest{PlanID: planID}).
					Return(model.CancelResult{Cancelled: []string{actionID}, Dependents: []string{}}, nil)
			},
			wantStatus: http.StatusOK, wantBody: `{"cancelled":["` + actionID + `"],"dependents_cancelled":[]}`,
		},
		{
			name: "pause", method: http.MethodPost, target: "/api/migration/pause",
			build: func(s *mocks.MigrationServiceMock) {
				s.SetPausedMock.Expect(minimock.AnyContext, true).Return(nil)
			},
			wantStatus: http.StatusOK, wantBody: `{"paused":true}`,
		},
		{
			name: "pin exists", method: http.MethodPost, target: "/api/migration/pins", body: `{"bucket":"b","key":"a/b","reason":"","author":"ops"}`,
			build: func(s *mocks.MigrationServiceMock) {
				s.AddPinMock.Expect(minimock.AnyContext, model.Pin{Bucket: "b", Key: "a/b", Author: "ops"}).Return(model.Pin{}, model.ErrPinExists)
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "unpin by query", method: http.MethodDelete, target: "/api/migration/pins?bucket=b&key=a%2Fb",
			build: func(s *mocks.MigrationServiceMock) {
				s.DeletePinMock.Expect(minimock.AnyContext, "b", "a/b").Return(nil)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "unknown pin", method: http.MethodDelete, target: "/api/migration/pins?bucket=b&key=k",
			build: func(s *mocks.MigrationServiceMock) {
				s.DeletePinMock.Return(model.ErrPinNotFound)
			},
			wantStatus: http.StatusNotFound,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			s := mocks.NewMigrationServiceMock(mc)
			if tt.build != nil {
				tt.build(s)
			}
			req := httptest.NewRequestWithContext(context.Background(), tt.method, tt.target, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			migration.NewHandler(s).ServeHTTP(rec, req)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			if tt.wantBody != "" {
				require.JSONEq(t, tt.wantBody, rec.Body.String())
			}
			if rec.Code >= 400 {
				var e map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &e))
				require.NotEmpty(t, e["error"])
			}
		})
	}
}
