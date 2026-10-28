package planner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

type cycleDeps struct {
	plans      *mocks.PlanRepositoryMock
	history    *mocks.HistoryRepositoryMock
	actions    *mocks.ActionRepositoryMock
	placements *mocks.PlacementClientMock
	migrations *mocks.MigrationServiceMock
	tx         *mocks.TxManagerMock
	finished   model.Plan
	recs       []model.Recommendation
}

var cycleNow = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func newCycleDeps(mc *minimock.Controller) *cycleDeps {
	d := &cycleDeps{
		plans: mocks.NewPlanRepositoryMock(mc), history: mocks.NewHistoryRepositoryMock(mc),
		actions: mocks.NewActionRepositoryMock(mc), placements: mocks.NewPlacementClientMock(mc),
		migrations: mocks.NewMigrationServiceMock(mc), tx: mocks.NewTxManagerMock(mc),
	}
	d.tx.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error { return f(ctx) })
	d.plans.GetSettingsMock.Return(model.PlannerSettings{
		Horizon: time.Hour, HotFixedMs: 1, HotMiBPerMs: 1, ColdFixedMs: 20, ColdMiBPerMs: 0.1,
		HotUtilization: 1, ColdUtilization: 1,
	}, nil)
	d.plans.CreatePlanMock.Set(func(_ context.Context, p model.Plan) (model.Plan, error) {
		p.Revision, p.Status = 7, model.PlanComputing
		return p, nil
	})
	d.plans.FinishPlanMock.Set(func(_ context.Context, p model.Plan, recs []model.Recommendation) error {
		d.finished, d.recs = p, recs
		return nil
	})
	d.plans.PrunePlansMock.Expect(minimock.AnyContext, 10).Return(nil)
	d.plans.GetPlanMock.Set(func(context.Context, string, bool) (model.Plan, error) { return d.finished, nil })
	return d
}

func (d *cycleDeps) service() *plannerService {
	n := 0
	ids := func() string {
		n++
		return "id-" + string(rune('a'+n))
	}
	return newService(Config{Interval: time.Minute, KeepPlans: 10, MaxRejectedSaved: -1}, d.plans, d.history, d.actions, d.tx,
		d.placements, d.migrations, features.NewService(), ewma.NewPredictor(), func() time.Time { return cycleNow }, ids)
}

func catalogEntry(id, key string, class model.StorageClass) model.Placement {
	return model.Placement{VersionID: id, Bucket: "b", Key: key, IsCurrent: true, Class: class, Generation: 1,
		SizeBytes: model.MiB, CreatedAt: cycleNow.Add(-48 * time.Hour), UpdatedAt: cycleNow.Add(-48 * time.Hour)}
}

// hot reads the version ten times a minute for the last half hour.
func hot(id, key string) []model.MinuteReads {
	var out []model.MinuteReads
	for m := 1; m <= 30; m++ {
		out = append(out, model.MinuteReads{VersionRef: model.VersionRef{Bucket: "b", ObjectKey: key, VersionID: id},
			Minute: cycleNow.Add(-time.Duration(m) * time.Minute), Reads: 10, Bytes: 10 * model.MiB})
	}
	return out
}

func (d *cycleDeps) world(coverage model.Coverage) {
	recent := catalogEntry("v-recent", "recent", model.StorageClassCold)
	recent.Generation, recent.UpdatedAt = 2, cycleNow.Add(-time.Minute)
	d.placements.ListCatalogMock.Return([]model.Placement{
		catalogEntry("v-idle", "idle", model.StorageClassHot),
		catalogEntry("v-popular", "popular", model.StorageClassCold),
		catalogEntry("v-pinned", "pinned", model.StorageClassCold),
		catalogEntry("v-busy", "busy", model.StorageClassCold),
		recent,
	}, nil)
	d.placements.ListStorageClassesMock.Return([]model.StorageClassUsage{
		{Class: model.StorageClassHot, CapacityBytes: 2 * model.MiB, MaxUtilization: 1, PlacedBytes: model.MiB},
		{Class: model.StorageClassCold, CapacityBytes: 100 * model.MiB, MaxUtilization: 1, PlacedBytes: 4 * model.MiB},
	}, nil)
	d.migrations.GetSettingsMock.Return(model.MigrationSettings{MinInterval: time.Hour}, nil)
	d.migrations.GetBudgetMock.Return(model.BudgetStatus{Remaining: 10 * model.MiB}, nil)
	d.migrations.ListPinsMock.Return([]model.Pin{{Bucket: "b", Key: "pinned"}}, nil)
	d.actions.ListActiveMock.Return([]repository.ActiveAction{
		{VersionID: "v-busy", TargetClass: model.StorageClassHot, SizeBytes: model.MiB, Status: model.ActionPending},
	}, nil)
	var reads []model.MinuteReads
	for _, k := range []string{"popular", "pinned", "busy", "recent"} {
		reads = append(reads, hot("v-"+k, k)...)
	}
	if coverage.Known && cycleNow.Sub(coverage.Watermark) <= time.Minute {
		d.history.ReadsSinceMock.Expect(minimock.AnyContext, coverage.Watermark.Add(-24*time.Hour), coverage.Watermark).Return(reads, nil)
	} else {
		d.placements.ListStorageClassesMock.Optional()
		d.migrations.GetSettingsMock.Optional()
		d.migrations.GetBudgetMock.Optional()
		d.migrations.ListPinsMock.Optional()
		d.actions.ListActiveMock.Optional()
	}
	d.history.CoverageMock.Return(coverage, nil)
}

func TestPlanCycle(t *testing.T) {
	known := model.Coverage{Known: true, Start: cycleNow.Add(-72 * time.Hour), Watermark: cycleNow}

	t.Run("snapshot becomes a ready plan with blockers and pending work counted", func(t *testing.T) {
		mc := minimock.NewController(t)
		d := newCycleDeps(mc)
		d.world(known)
		plan, err := d.service().Plan(context.Background(), model.TriggerManual)
		require.NoError(t, err)
		require.Equal(t, model.PlanReady, plan.Status)
		require.Equal(t, model.TriggerManual, plan.Trigger)
		require.Equal(t, ewma.MethodVersion, plan.MethodVersion)

		got := map[string]model.Recommendation{}
		for _, r := range d.recs {
			require.Equal(t, plan.PlanID, r.PlanID)
			got[r.VersionID] = r
		}
		// The pending promotion of v-busy already takes the free HOT slot, so the
		// popular version displaces the idle one.
		require.True(t, got["v-popular"].Selected)
		require.Equal(t, []string{got["v-idle"].RecommendationID}, got["v-popular"].DependsOn)
		require.Equal(t, model.KindEvict, got["v-idle"].Kind)
		require.Equal(t, model.RejectPinned, got["v-pinned"].Reason)
		require.Equal(t, model.RejectInProgress, got["v-busy"].Reason)
		require.Equal(t, model.RejectMinInterval, got["v-recent"].Reason)
		// Budget left after the pending action: 10 MiB - 1 MiB.
		require.Equal(t, int64(9*model.MiB), got["v-idle"].Explanation.Resources.BudgetBefore)
		f := got["v-popular"].Explanation.Forecast
		require.Equal(t, int64(300), f.Reads1h)
		require.Greater(t, f.ExpectedReads, 0.0)
		require.InDelta(t, 1.0, f.ReadFraction, 1e-9)
	})

	t.Run("unknown completeness stops planning without recommendations", func(t *testing.T) {
		mc := minimock.NewController(t)
		d := newCycleDeps(mc)
		d.world(model.Coverage{})
		plan, err := d.service().Plan(context.Background(), model.TriggerTimer)
		require.NoError(t, err)
		require.Equal(t, model.PlanHistoryIncomplete, plan.Status)
		require.Equal(t, "unknown_completeness", plan.Reason)
		require.Empty(t, d.recs)
	})

	t.Run("detected gap stops planning", func(t *testing.T) {
		mc := minimock.NewController(t)
		d := newCycleDeps(mc)
		gap := known
		gap.Gaps = []model.Interval{{Start: cycleNow.Add(-2 * time.Hour), End: cycleNow.Add(-90 * time.Minute)}}
		d.world(gap)
		plan, err := d.service().Plan(context.Background(), model.TriggerTimer)
		require.NoError(t, err)
		require.Equal(t, model.PlanHistoryIncomplete, plan.Status)
		require.Equal(t, "gap", plan.Reason)
	})

	t.Run("Metadata failure is stored as a failed plan", func(t *testing.T) {
		mc := minimock.NewController(t)
		d := newCycleDeps(mc)
		down := errors.New("Metadata unavailable")
		d.placements.ListCatalogMock.Return(nil, down)
		plan, err := d.service().Plan(context.Background(), model.TriggerTimer)
		require.ErrorIs(t, err, down)
		require.Equal(t, model.PlanFailed, plan.Status)
		require.Contains(t, plan.Reason, "Metadata unavailable")
	})
}

func TestPlanHistoryLag(t *testing.T) {
	for _, tt := range []struct {
		name   string
		lag    time.Duration
		status model.PlanStatus
	}{{"calculates at W", 30 * time.Second, model.PlanReady}, {"threshold is inclusive", 60 * time.Second, model.PlanReady}, {"stale history stops planning", 61 * time.Second, model.PlanHistoryIncomplete}} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newCycleDeps(mc)
			w := cycleNow.Add(-tt.lag)
			d.world(model.Coverage{Known: true, Start: cycleNow.Add(-72 * time.Hour), Watermark: w})
			p, err := d.service().Plan(context.Background(), model.TriggerManual)
			require.NoError(t, err)
			require.Equal(t, tt.status, p.Status)
			if tt.status == model.PlanHistoryIncomplete {
				require.Contains(t, p.Reason, "история отстаёт: 1m1s")
				require.Empty(t, d.recs)
			} else if tt.lag == 30*time.Second {
				for _, r := range d.recs {
					if r.VersionID == "v-popular" {
						require.NotNil(t, r.Explanation.Forecast.LastReadSeconds)
						require.Equal(t, 30.0, *r.Explanation.Forecast.LastReadSeconds)
					}
				}
			}
		})
	}
}
