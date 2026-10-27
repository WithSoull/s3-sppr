package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

// TestResumeInterruptedAction continues a running action left by a restart from
// the copies Metadata recorded for its migration_id.
func TestResumeInterruptedAction(t *testing.T) {
	now := time.Now()
	withState := func(state model.CopyState) []model.PlacementCopy {
		target := reserved()
		target.State = state
		return []model.PlacementCopy{target}
	}
	cases := []struct {
		name   string
		action *model.Action
		build  func(*deps)
		want   []step
	}{
		{
			name:   "no copies yet starts from the checks",
			action: running(model.StepReserving, model.ReasonNone, true),
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.sink(nil)
				switches(d, nil)
			},
			want: []step{copying(), switching(), succeeded()},
		},
		{
			name:   "reserved before the step was recorded reuses the claim charge",
			action: running(model.StepReserving, model.ReasonNone, true),
			build: func(d *deps) {
				d.placements.ListMigrationCopiesMock.Return(withState(model.CopyReserved), nil)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.source(content)
				d.sink(nil)
				switches(d, nil)
			},
			want: []step{copying(), switching(), succeeded()},
		},
		{
			name:   "interrupted copy keeps its charge and copies again under the same blob_id",
			action: running(model.StepCopying, model.ReasonNone, true),
			build: func(d *deps) {
				d.placements.ListMigrationCopiesMock.Return(withState(model.CopyReserved), nil)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.source(content)
				d.sink(nil)
				switches(d, nil)
			},
			want: []step{
				{Status: model.ActionRunning, Step: model.StepCopying, Settle: true, Bytes: size, Reopen: true},
				switching(), succeeded(),
			},
		},
		{
			name:   "interrupted before the switch copies again with a new charge",
			action: running(model.StepSwitching, model.ReasonNone, false),
			build: func(d *deps) {
				d.placements.ListMigrationCopiesMock.Return(withState(model.CopyReserved), nil)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.source(content)
				d.sink(nil)
				switches(d, nil)
			},
			want: []step{
				{Status: model.ActionRunning, Step: model.StepCopying, Reopen: true},
				switching(), succeeded(),
			},
		},
		{
			name:   "promoted target means the switch happened",
			action: running(model.StepSwitching, model.ReasonNone, false),
			build: func(d *deps) {
				d.placements.ListMigrationCopiesMock.Return(withState(model.CopyPromoted), nil)
			},
			want: []step{{Status: model.ActionSucceeded, Settle: true}},
		},
		{
			name:   "aborted target without a recorded reason fails as internal",
			action: running(model.StepCopying, model.ReasonNone, true),
			build: func(d *deps) {
				d.placements.ListMigrationCopiesMock.Return(withState(model.CopyCleanupPending), nil)
			},
			want: []step{{Status: model.ActionFailed, Reason: model.ReasonInternal, Settle: true, Bytes: size}},
		},
		{
			name:   "aborting action finishes the abort with its reason",
			action: running(model.StepAborting, model.ReasonNoCapacity, false),
			build: func(d *deps) {
				aborts(d, model.ReasonNoCapacity, nil)
			},
			want: []step{failed(model.ReasonNoCapacity)},
		},
		{
			name:   "placement changed while the process was down",
			action: running(model.StepCopying, model.ReasonNone, true),
			build: func(d *deps) {
				d.placements.ListMigrationCopiesMock.Return(withState(model.CopyReserved), nil)
				p := placement(2, now)
				p.Class = model.StorageClassHot
				d.placements.GetPlacementMock.Return(p, nil)
				aborts(d, model.ReasonStale, nil)
			},
			want: []step{aborting(model.ReasonStale, size), failed(model.ReasonStale)},
		},
		{
			name:   "deleted object fails as stale",
			action: running(model.StepCopying, model.ReasonNone, true),
			build: func(d *deps) {
				d.placements.ListMigrationCopiesMock.Return(withState(model.CopyReserved), nil)
				d.placements.GetPlacementMock.Return(model.Placement{}, model.ErrUpstreamNotFound)
				aborts(d, model.ReasonStale, nil)
			},
			want: []step{aborting(model.ReasonStale, size), failed(model.ReasonStale)},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newDeps(mc, tt.action)
			tt.build(d)
			worked, err := d.executor().Step(context.Background())
			require.True(t, worked)
			require.NoError(t, err)
			require.Equal(t, tt.want, d.recorded)
		})
	}
}

func TestStepClaim(t *testing.T) {
	cases := []struct {
		name       string
		settings   model.MigrationSettings
		spent      int64
		wantBudget int64
	}{
		{name: "claims within the remaining budget", settings: model.MigrationSettings{BudgetBytes: 100, BudgetWindow: time.Hour}, spent: 30, wantBudget: 70},
		{name: "paused does not claim", settings: model.MigrationSettings{BudgetBytes: 100, BudgetWindow: time.Hour, Paused: true}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newDeps(mc, nil)
			d.actions.ListRunningMock.Return(nil, nil)
			d.settings.GetForUpdateMock.Return(tt.settings, nil)
			if !tt.settings.Paused {
				d.settings.SpentMock.Expect(minimock.AnyContext, time.Hour).Return(tt.spent, nil)
				d.actions.ClaimMock.Expect(minimock.AnyContext, tt.wantBudget).Return(nil, nil)
			}
			worked, err := d.executor().Step(context.Background())
			require.NoError(t, err)
			require.False(t, worked)
		})
	}
}

func TestStepResumesBeforeClaiming(t *testing.T) {
	mc := minimock.NewController(t)
	d := newDeps(mc, running(model.StepSwitching, model.ReasonNone, false))
	d.placements.ListMigrationCopiesMock.Return([]model.PlacementCopy{func() model.PlacementCopy {
		c := reserved()
		c.State = model.CopyPromoted
		return c
	}()}, nil)
	worked, err := d.executor().Step(context.Background())
	require.NoError(t, err)
	require.True(t, worked)
	require.Equal(t, uint64(0), d.settings.GetForUpdateAfterCounter(), "a resumed action does not wait for the claim checks")
	require.Equal(t, uint64(0), d.actions.ClaimAfterCounter())
}
