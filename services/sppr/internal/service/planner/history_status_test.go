package planner

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestObjectHistoryStatus(t *testing.T) {
	for _, tt := range []struct {
		name          string
		known         bool
		lag           time.Duration
		gaps          []model.Interval
		state, reason string
	}{
		{name: "complete zero demand", known: true, state: "чтений нет", reason: "complete"},
		{name: "gap is not zero demand", known: true, gaps: []model.Interval{{Start: cycleNow.Add(-time.Hour), End: cycleNow.Add(-time.Minute)}}, state: "история неполна", reason: "gap"},
		{name: "stale zero demand is incomplete", known: true, lag: 61 * time.Second, state: "история неполна", reason: "история отстаёт"},
		{name: "unknown history", state: "история неполна", reason: "unknown_completeness"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			p := mocks.NewPlacementClientMock(mc)
			h := mocks.NewHistoryRepositoryMock(mc)
			m := mocks.NewMigrationServiceMock(mc)
			plans := mocks.NewPlanRepositoryMock(mc)
			a := mocks.NewActionRepositoryMock(mc)
			p.ListStorageClassesMock.Return(nil, nil)
			p.ListCatalogMock.Return([]model.Placement{catalogEntry("v", "idle", model.StorageClassCold)}, nil)
			c := model.Coverage{Known: tt.known, Start: cycleNow.Add(-72 * time.Hour), Watermark: cycleNow.Add(-tt.lag), Gaps: tt.gaps, OpenStreams: 2, Gaps24h: int64(len(tt.gaps))}
			h.CoverageMock.Return(c, nil)
			if tt.known {
				h.ReadsSinceMock.Expect(minimock.AnyContext, c.Watermark.Add(-24*time.Hour), c.Watermark).Return(nil, nil)
			}
			m.GetSettingsMock.Return(model.MigrationSettings{}, nil)
			m.GetBudgetMock.Return(model.BudgetStatus{}, nil)
			plans.LatestPlanMock.Return(nil, nil)
			a.ListActiveMock.Return([]repository.ActiveAction{}, nil)
			s := newService(Config{}, plans, h, a, nil, p, m, features.NewService(), ewma.NewPredictor(), func() time.Time { return cycleNow }, func() string { return "id" })
			st, err := s.Status(context.Background())
			require.NoError(t, err)
			require.Equal(t, c.OpenStreams, st.History.OpenStreams)
			require.Len(t, st.Objects, 1)
			require.Equal(t, tt.state, st.Objects[0].State)
			require.Equal(t, tt.reason, st.Objects[0].Quality.Reason)
			require.Equal(t, tt.lag.Seconds(), st.HistoryLagSeconds)
		})
	}
}

func TestHistoryStartAdjustmentPreservesWatermark(t *testing.T) {
	w := cycleNow.Add(-time.Second)
	c := model.Coverage{Known: true, Start: cycleNow.Add(-time.Hour), Watermark: w}
	versions := []model.Version{{CreatedAt: cycleNow.Add(-48 * time.Hour)}}
	got := extendHistoryStart(c, versions)
	require.Equal(t, w, got.Watermark)
	require.Equal(t, versions[0].CreatedAt, got.Start)
}
func TestCatalogueNewerThanWatermarkIsIncomplete(t *testing.T) {
	mc := minimock.NewController(t)
	h := mocks.NewHistoryRepositoryMock(mc)
	h.ReadsSinceMock.Expect(minimock.AnyContext, cycleNow.Add(-24*time.Hour), cycleNow).Return(nil, nil)
	s := plannerService{history: h, features: features.NewService()}
	p := catalogEntry("v", "new", model.StorageClassCold)
	p.CreatedAt = cycleNow.Add(time.Second)
	_, q, err := s.forecastFeatures(context.Background(), cycleNow, []model.Placement{p}, model.Coverage{Known: true, Start: cycleNow.Add(-time.Hour), Watermark: cycleNow})
	require.NoError(t, err)
	require.False(t, q.Usable)
	require.Equal(t, "unobserved", q.Reason)
}

func TestStatusIncludesGapsAtStartOfWatermarkWindow(t *testing.T) {
	mc := minimock.NewController(t)
	p := mocks.NewPlacementClientMock(mc)
	h := mocks.NewHistoryRepositoryMock(mc)
	m := mocks.NewMigrationServiceMock(mc)
	plans := mocks.NewPlanRepositoryMock(mc)
	a := mocks.NewActionRepositoryMock(mc)
	w := cycleNow.Add(-30 * time.Second)
	c := model.Coverage{Known: true, Start: cycleNow.Add(-72 * time.Hour), Watermark: w}
	h.CoverageMock.Set(func(_ context.Context, since time.Time) (model.Coverage, error) {
		copy := c
		if since.Equal(w.Add(-24 * time.Hour)) {
			copy.Gaps = []model.Interval{{Start: w.Add(-24 * time.Hour), End: cycleNow.Add(-24*time.Hour - time.Second)}}
		}
		return copy, nil
	})
	h.ReadsSinceMock.Expect(minimock.AnyContext, w.Add(-24*time.Hour), w).Return(nil, nil)
	p.ListStorageClassesMock.Return(nil, nil)
	p.ListCatalogMock.Return([]model.Placement{catalogEntry("v", "idle", model.StorageClassCold)}, nil)
	m.GetSettingsMock.Return(model.MigrationSettings{}, nil)
	m.GetBudgetMock.Return(model.BudgetStatus{}, nil)
	plans.LatestPlanMock.Return(nil, nil)
	a.ListActiveMock.Return(nil, nil)
	s := newService(Config{}, plans, h, a, nil, p, m, features.NewService(), ewma.NewPredictor(), func() time.Time { return cycleNow }, func() string { return "id" })
	st, err := s.Status(context.Background())
	require.NoError(t, err)
	require.False(t, st.HistoryQuality.Usable)
	require.Len(t, st.History.Gaps, 1)
	require.Equal(t, "gap", st.HistoryQuality.Reason)
	require.Equal(t, "история неполна", st.Objects[0].State)
}
