package planner

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

const (
	cmdID   = "0b6f6c55-0000-4000-8000-000000000001"
	planID  = "0b6f6c55-0000-4000-8000-0000000000a1"
	newerID = "0b6f6c55-0000-4000-8000-0000000000a2"
	evictID = "0b6f6c55-0000-4000-8000-0000000000e1"
	promID  = "0b6f6c55-0000-4000-8000-0000000000f1"
	freeID  = "0b6f6c55-0000-4000-8000-0000000000f2"
	userID  = "admin"
)

type cmdDeps struct {
	plans      *mocks.PlanRepositoryMock
	actions    *mocks.ActionRepositoryMock
	migrations *mocks.MigrationServiceMock
	tx         *mocks.TxManagerMock
	stored     []model.Command
}

func newCmdDeps(mc *minimock.Controller) *cmdDeps {
	d := &cmdDeps{
		plans:      mocks.NewPlanRepositoryMock(mc),
		actions:    mocks.NewActionRepositoryMock(mc),
		migrations: mocks.NewMigrationServiceMock(mc),
		tx:         mocks.NewTxManagerMock(mc),
	}
	d.tx.ReadCommittedMock.Optional().Set(func(ctx context.Context, f db.Handler) error { return f(ctx) })
	d.plans.InsertCommandMock.Optional().Set(func(_ context.Context, cmd model.Command) error {
		d.stored = append(d.stored, cmd)
		return nil
	})
	return d
}

func (d *cmdDeps) service() *plannerService {
	now := func() time.Time { return time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC) }
	return newService(Config{Interval: time.Minute, KeepPlans: 10}, d.plans, nil, d.actions, d.tx, nil, d.migrations, nil, nil, now, func() string { return "" })
}

func (d *cmdDeps) fresh() {
	d.plans.GetCommandMock.Expect(minimock.AnyContext, cmdID).Return(model.Command{}, model.ErrCommandNotFound)
}

func (d *cmdDeps) planIs(status model.PlanStatus, latest string) {
	d.plans.GetPlanMock.Expect(minimock.AnyContext, planID, true).Return(model.Plan{PlanID: planID, Revision: 1, Status: status}, nil)
	d.plans.LatestPlanMock.Return(&model.Plan{PlanID: latest, Revision: 2}, nil)
}

// The plan displaces evict to make room for prom; free fits without displacement.
func planRecs() []model.Recommendation {
	return []model.Recommendation{
		{RecommendationID: evictID, Order: 0, Kind: model.KindEvict, VersionID: "v-e", ExpectedGeneration: 1,
			SourceClass: model.StorageClassHot, TargetClass: model.StorageClassCold, SizeBytes: 10},
		{RecommendationID: promID, Order: 1, Kind: model.KindPromote, VersionID: "v-p", ExpectedGeneration: 3,
			SourceClass: model.StorageClassCold, TargetClass: model.StorageClassHot, SizeBytes: 10, DependsOn: []string{evictID}},
		{RecommendationID: freeID, Order: 2, Kind: model.KindPromote, VersionID: "v-f", ExpectedGeneration: 1,
			SourceClass: model.StorageClassCold, TargetClass: model.StorageClassHot, SizeBytes: 5},
	}
}

func spec(r model.Recommendation) model.ActionSpec {
	return model.ActionSpec{ActionID: r.RecommendationID, PlanID: planID, VersionID: r.VersionID,
		ExpectedGeneration: r.ExpectedGeneration, SourceClass: r.SourceClass, TargetClass: r.TargetClass,
		SizeBytes: r.SizeBytes, DependsOn: r.DependsOn}
}

func confirmReq(ids ...string) model.CommandRequest {
	return model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandConfirm, PlanID: planID, RecommendationIDs: ids}
}

func TestExecuteConfirm(t *testing.T) {
	recs := planRecs()
	cases := []struct {
		name    string
		req     model.CommandRequest
		build   func(*cmdDeps)
		want    model.Command
		wantErr error
	}{
		{
			name: "whole plan goes to the executor in plan order",
			req:  confirmReq(),
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanReady, planID)
				d.plans.SelectedRecommendationsMock.Expect(minimock.AnyContext, planID).Return(recs, nil)
				d.migrations.SubmitActionsMock.Expect(minimock.AnyContext, planID, []model.ActionSpec{spec(recs[0]), spec(recs[1]), spec(recs[2])}).
					Return(model.SubmitResult{}, nil)
				d.plans.SetPlanStatusMock.Expect(minimock.AnyContext, planID, model.PlanConfirmed).Return(nil)
			},
			want: model.Command{Status: model.CommandAccepted, Actions: []string{evictID, promID, freeID}},
		},
		{
			name: "independent part is accepted",
			req:  confirmReq(freeID),
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanReady, planID)
				d.plans.SelectedRecommendationsMock.Return(recs, nil)
				d.migrations.SubmitActionsMock.Expect(minimock.AnyContext, planID, []model.ActionSpec{spec(recs[2])}).Return(model.SubmitResult{}, nil)
				d.plans.SetPlanStatusMock.Return(nil)
			},
			want: model.Command{Status: model.CommandAccepted, Actions: []string{freeID}},
		},
		{
			name: "promotion without the eviction it needs is refused",
			req:  confirmReq(promID),
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanReady, planID)
				d.plans.SelectedRecommendationsMock.Return(recs, nil)
				d.actions.GetManyMock.Expect(minimock.AnyContext, []string{evictID}).Return(map[string]*model.Action{}, nil)
			},
			want: model.Command{Status: model.CommandRejected, Code: "DEPENDENCY_REQUIRED", Missing: []string{evictID}},
		},
		{
			name: "eviction confirmed earlier counts as present",
			req:  confirmReq(promID),
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanConfirmed, planID)
				d.plans.SelectedRecommendationsMock.Return(recs, nil)
				d.actions.GetManyMock.Return(map[string]*model.Action{evictID: {ActionSpec: model.ActionSpec{ActionID: evictID, PlanID: planID}}}, nil)
				d.migrations.SubmitActionsMock.Expect(minimock.AnyContext, planID, []model.ActionSpec{spec(recs[1])}).Return(model.SubmitResult{}, nil)
				d.plans.SetPlanStatusMock.Return(nil)
			},
			want: model.Command{Status: model.CommandAccepted, Actions: []string{promID}},
		},
		{
			name: "older revision is stale and nothing is submitted",
			req:  confirmReq(),
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanSuperseded, newerID)
			},
			want: model.Command{Status: model.CommandRejected, Code: "PLAN_STALE"},
		},
		{
			name: "rejected plan cannot be confirmed",
			req:  confirmReq(),
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanRejected, planID)
			},
			want: model.Command{Status: model.CommandRejected, Code: "PLAN_STALE"},
		},
		{
			name: "recommendation outside the plan is a client error",
			req:  confirmReq("0b6f6c55-0000-4000-8000-0000000000ff"),
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanReady, planID)
				d.plans.SelectedRecommendationsMock.Return(recs, nil)
			},
			wantErr: model.ErrInvalidArgument,
		},
		{
			name:    "command_id must be a UUID",
			req:     model.CommandRequest{CommandID: "x", Initiator: userID, Type: model.CommandConfirm, PlanID: planID},
			build:   func(*cmdDeps) {},
			wantErr: model.ErrInvalidArgument,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newCmdDeps(mc)
			tt.build(d)
			got, err := d.service().Execute(context.Background(), tt.req)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Empty(t, d.stored)
				return
			}
			require.NoError(t, err)
			require.Len(t, d.stored, 1, "every decision is journaled")
			require.Equal(t, got, d.stored[0])
			require.Equal(t, tt.want.Status, got.Status)
			require.Equal(t, tt.want.Code, got.Code)
			require.Equal(t, tt.want.Missing, got.Missing)
			require.Equal(t, tt.want.Actions, got.Actions)
			require.Equal(t, userID, got.Initiator)
		})
	}
}

func TestExecuteRetry(t *testing.T) {
	stored := model.Command{CommandRequest: confirmReq(freeID), Status: model.CommandAccepted, Actions: []string{freeID}}
	cases := []struct {
		name    string
		req     model.CommandRequest
		wantErr error
	}{
		{name: "same command returns the first result", req: confirmReq(freeID, freeID)},
		{name: "other selection under the same ID conflicts", req: confirmReq(promID), wantErr: model.ErrCommandConflict},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newCmdDeps(mc)
			d.plans.GetCommandMock.Return(stored, nil)
			got, err := d.service().Execute(context.Background(), tt.req)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, stored, got)
			require.Empty(t, d.stored, "a retry must not act again")
		})
	}
}

func TestExecuteOtherCommands(t *testing.T) {
	good := model.PlannerSettings{Horizon: time.Hour, HotFixedMs: 1, HotMiBPerMs: 1, ColdFixedMs: 20, ColdMiBPerMs: 0.1, HotUtilization: 0.9, ColdUtilization: 0.9}
	bad := good
	bad.HotUtilization = 0
	cases := []struct {
		name    string
		req     model.CommandRequest
		build   func(*cmdDeps)
		want    model.Command
		wantErr error
	}{
		{
			name: "reject the latest ready plan",
			req:  model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandReject, PlanID: planID},
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanReady, planID)
				d.plans.SetPlanStatusMock.Expect(minimock.AnyContext, planID, model.PlanRejected).Return(nil)
			},
			want: model.Command{Status: model.CommandAccepted},
		},
		{
			name: "confirmed plan cannot be rejected",
			req:  model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandReject, PlanID: planID},
			build: func(d *cmdDeps) {
				d.fresh()
				d.planIs(model.PlanConfirmed, planID)
			},
			want: model.Command{Status: model.CommandRejected, Code: "PLAN_STALE"},
		},
		{
			name: "cancel pending actions of a plan",
			req:  model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandCancelPending, PlanID: planID},
			build: func(d *cmdDeps) {
				d.fresh()
				d.migrations.CancelActionsMock.Expect(minimock.AnyContext, model.CancelRequest{PlanID: planID}).
					Return(model.CancelResult{Cancelled: []string{evictID}, Dependents: []string{promID}}, nil)
			},
			want: model.Command{Status: model.CommandAccepted, Actions: []string{evictID, promID}},
		},
		{
			name: "pause stops new migrations",
			req:  model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandPause},
			build: func(d *cmdDeps) {
				d.fresh()
				d.migrations.SetPausedMock.Expect(minimock.AnyContext, true).Return(nil)
			},
			want: model.Command{Status: model.CommandAccepted},
		},
		{
			name: "settings are stored",
			req:  model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandSettings, Settings: &good},
			build: func(d *cmdDeps) {
				d.fresh()
				d.plans.UpdateSettingsMock.Expect(minimock.AnyContext, good).Return(nil)
			},
			want: model.Command{Status: model.CommandAccepted},
		},
		{
			name:    "invalid settings are refused before anything is stored",
			req:     model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandSettings, Settings: &bad},
			build:   func(*cmdDeps) {},
			wantErr: model.ErrInvalidArgument,
		},
		{
			name:    "cancel needs a plan or actions",
			req:     model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandCancelPending},
			build:   func(*cmdDeps) {},
			wantErr: model.ErrInvalidArgument,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newCmdDeps(mc)
			tt.build(d)
			got, err := d.service().Execute(context.Background(), tt.req)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Empty(t, d.stored)
				return
			}
			require.NoError(t, err)
			require.Len(t, d.stored, 1)
			require.Equal(t, tt.want.Status, got.Status)
			require.Equal(t, tt.want.Code, got.Code)
			require.Equal(t, tt.want.Actions, got.Actions)
		})
	}
}

func TestExecuteRecomputeWakesTheCycle(t *testing.T) {
	mc := minimock.NewController(t)
	d := newCmdDeps(mc)
	d.fresh()
	s := d.service()
	got, err := s.Execute(context.Background(), model.CommandRequest{CommandID: cmdID, Initiator: userID, Type: model.CommandRecompute})
	require.NoError(t, err)
	require.Equal(t, model.CommandAccepted, got.Status)
	require.Equal(t, cmdID, <-s.recompute)
}
