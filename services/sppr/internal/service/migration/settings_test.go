package migration_test

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/migration"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

var defaults = model.MigrationSettings{BudgetBytes: 100, BudgetWindow: time.Hour, MinInterval: time.Hour}

func TestListActionsBlockedBy(t *testing.T) {
	pending := func(size int64, depsPending bool) *model.Action {
		return &model.Action{ActionSpec: model.ActionSpec{ActionID: actionA, SizeBytes: size}, Status: model.ActionPending, DepsPending: depsPending}
	}
	cases := []struct {
		name     string
		action   *model.Action
		settings model.MigrationSettings
		want     model.BlockedBy
	}{
		{"runnable", pending(10, false), defaults, model.BlockedByNone},
		{"paused", pending(10, true), model.MigrationSettings{BudgetBytes: 100, BudgetWindow: time.Hour, Paused: true}, model.BlockedByPaused},
		{"dependencies", pending(10, true), defaults, model.BlockedByDependencies},
		{"budget", pending(31, false), defaults, model.BlockedByBudget},
		{"not pending", &model.Action{ActionSpec: model.ActionSpec{ActionID: actionA, SizeBytes: 500}, Status: model.ActionRunning}, defaults, model.BlockedByNone},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			actions := mocks.NewActionRepositoryMock(mc)
			actions.ListMock.Expect(minimock.AnyContext, model.ActionFilter{PageSize: 101}).Return([]*model.Action{tt.action}, nil)
			settings := mocks.NewSettingsRepositoryMock(mc)
			if tt.action.Status == model.ActionPending {
				settings.GetMock.Return(tt.settings, nil)
				settings.SpentMock.Expect(minimock.AnyContext, time.Hour).Return(70, nil)
			}
			s := migration.NewService(actions, settings, mocks.NewPinRepositoryMock(mc), mocks.NewTxManagerMock(mc), nil)
			page, err := s.ListActions(context.Background(), model.ActionFilter{})
			require.NoError(t, err)
			require.Len(t, page.Actions, 1)
			require.Equal(t, tt.want, page.Actions[0].BlockedBy)
		})
	}
}

func TestListActionsPagination(t *testing.T) {
	mc := minimock.NewController(t)
	actions := mocks.NewActionRepositoryMock(mc)
	actions.ListMock.Expect(minimock.AnyContext, model.ActionFilter{PlanID: planID, Status: model.ActionSucceeded, PageSize: 3, PageToken: actionA}).
		Return([]*model.Action{
			{ActionSpec: model.ActionSpec{ActionID: actionB}, Status: model.ActionSucceeded},
			{ActionSpec: model.ActionSpec{ActionID: actionC}, Status: model.ActionSucceeded},
			{ActionSpec: model.ActionSpec{ActionID: version}, Status: model.ActionSucceeded},
		}, nil)
	s := migration.NewService(actions, mocks.NewSettingsRepositoryMock(mc), mocks.NewPinRepositoryMock(mc), mocks.NewTxManagerMock(mc), nil)
	page, err := s.ListActions(context.Background(), model.ActionFilter{PlanID: planID, Status: model.ActionSucceeded, PageSize: 2, PageToken: actionA})
	require.NoError(t, err)
	require.Len(t, page.Actions, 2)
	require.Equal(t, actionC, page.NextPageToken)

	for _, bad := range []model.ActionFilter{{PlanID: "x"}, {Status: "done"}, {PageSize: 1001}, {PageToken: "x"}} {
		_, err := s.ListActions(context.Background(), bad)
		require.ErrorIs(t, err, model.ErrInvalidArgument)
	}
}

func TestSettingsChanges(t *testing.T) {
	cases := []struct {
		name    string
		change  func(context.Context, service.MigrationService) error
		want    model.MigrationSettings
		wantErr bool
	}{
		{
			name: "budget",
			change: func(ctx context.Context, s service.MigrationService) error {
				return s.SetBudget(ctx, 5<<30, 2*time.Hour)
			},
			want: model.MigrationSettings{BudgetBytes: 5 << 30, BudgetWindow: 2 * time.Hour, MinInterval: time.Hour},
		},
		{
			name:   "min interval",
			change: func(ctx context.Context, s service.MigrationService) error { return s.SetMinInterval(ctx, 0) },
			want:   model.MigrationSettings{BudgetBytes: 100, BudgetWindow: time.Hour},
		},
		{
			name:   "pause",
			change: func(ctx context.Context, s service.MigrationService) error { return s.SetPaused(ctx, true) },
			want:   model.MigrationSettings{BudgetBytes: 100, BudgetWindow: time.Hour, MinInterval: time.Hour, Paused: true},
		},
		{
			name:    "zero budget",
			change:  func(ctx context.Context, s service.MigrationService) error { return s.SetBudget(ctx, 0, time.Hour) },
			wantErr: true,
		},
		{
			name: "window below a second",
			change: func(ctx context.Context, s service.MigrationService) error {
				return s.SetBudget(ctx, 1, time.Millisecond)
			},
			wantErr: true,
		},
		{
			name: "negative interval",
			change: func(ctx context.Context, s service.MigrationService) error {
				return s.SetMinInterval(ctx, -time.Second)
			},
			wantErr: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			settings := mocks.NewSettingsRepositoryMock(mc)
			tx := mocks.NewTxManagerMock(mc)
			if !tt.wantErr {
				tx.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error { return f(ctx) })
				settings.GetForUpdateMock.Return(defaults, nil)
				settings.UpdateMock.Expect(minimock.AnyContext, tt.want).Return(nil)
			}
			s := migration.NewService(mocks.NewActionRepositoryMock(mc), settings, mocks.NewPinRepositoryMock(mc), tx, nil)
			err := tt.change(context.Background(), s)
			if tt.wantErr {
				require.ErrorIs(t, err, model.ErrInvalidArgument)
				return
			}
			require.NoError(t, err)
		})
	}
}
