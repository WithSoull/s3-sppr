package planner

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func mlFixture(t *testing.T) service.Predictor {
	t.Helper()
	p, err := lgbm.Load(filepath.Join("..", "predictor", "lgbm", "testdata"), time.Hour)
	require.NoError(t, err)
	return p
}

func TestPlanML(t *testing.T) {
	for _, tt := range []struct {
		name       string
		horizon    time.Duration
		noForecast bool
	}{
		{"ML plan stores model identity and explanation", time.Hour, false},
		{"changed horizon rejects object forecasts", 2 * time.Hour, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newCycleDeps(mc)
			d.world(model.Coverage{Known: true, Start: cycleNow.Add(-72 * time.Hour), Watermark: cycleNow})
			d.plans.GetSettingsMock.Return(model.PlannerSettings{Horizon: tt.horizon, HotFixedMs: 1, HotMiBPerMs: 1, ColdFixedMs: 20, ColdMiBPerMs: 0.1, HotUtilization: 1, ColdUtilization: 1}, nil)
			s := d.service()
			s.predictor = mlFixture(t)
			plan, err := s.Plan(context.Background(), model.TriggerManual)
			require.NoError(t, err)
			require.Equal(t, model.PlanReady, plan.Status)
			require.Equal(t, lgbm.MethodVersion, plan.MethodVersion)
			require.Equal(t, "spike-fixture-v1", plan.ModelVersion)
			require.NotEmpty(t, d.recs)
			encoded, err := json.Marshal(d.recs)
			require.NoError(t, err)
			if tt.noForecast {
				require.NotContains(t, string(encoded), `"hurdle"`)
			} else {
				for _, field := range []string{`"hurdle"`, `"probability"`, `"conditional_reads"`, `"expected_reads"`, `"ewma_expected_reads"`} {
					require.Contains(t, string(encoded), field)
				}
			}
			found := false
			for _, r := range d.recs {
				if r.VersionID != "v-popular" {
					continue
				}
				found = true
				f := r.Explanation.Forecast
				if tt.noForecast {
					require.Equal(t, model.RejectNoForecast, r.Reason)
					require.Nil(t, f.Hurdle)
					require.False(t, r.Selected)
					continue
				}
				require.True(t, r.Selected)
				require.Equal(t, lgbm.MethodVersion, f.Method)
				require.NotNil(t, f.Hurdle)
				require.Equal(t, f.ExpectedReads, f.Hurdle.Probability*f.Hurdle.ConditionalReads)
				require.Equal(t, f.EWMA1h, f.Hurdle.EWMAExpectedReads)
				require.Greater(t, f.Hurdle.EWMAExpectedReads, 0.0)
				// The cost calculation uses the same forecast displayed in the card.
				require.InDelta(t, f.ExpectedReads*(20-1)+f.ExpectedMiB*(1/0.1-1), r.Explanation.Effect.GainMs, 1e-9)
			}
			require.True(t, found)
		})
	}
}

func TestPredictorStatus(t *testing.T) {
	for _, tt := range []struct {
		name   string
		ml     bool
		reason string
	}{
		{"ML", true, ""}, {"EWMA fallback", false, "manifest.json: no such file"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			placements := mocks.NewPlacementClientMock(mc)
			history := mocks.NewHistoryRepositoryMock(mc)
			migrations := mocks.NewMigrationServiceMock(mc)
			plans := mocks.NewPlanRepositoryMock(mc)
			actions := mocks.NewActionRepositoryMock(mc)
			placements.ListStorageClassesMock.Return(nil, nil)
			history.CoverageMock.Return(model.Coverage{}, nil)
			migrations.GetSettingsMock.Return(model.MigrationSettings{}, nil)
			migrations.GetBudgetMock.Return(model.BudgetStatus{}, nil)
			plans.LatestPlanMock.Return(nil, nil)
			actions.ListActiveMock.Return([]repository.ActiveAction{}, nil)
			p := ewma.NewPredictor()
			if tt.ml {
				p = mlFixture(t)
			}
			s := NewService(Config{FallbackReason: tt.reason}, plans, history, actions, nil, placements, migrations, nil, p)
			st, err := s.Status(context.Background())
			require.NoError(t, err)
			require.Equal(t, p.MethodVersion(), st.MethodVersion)
			require.Equal(t, p.ModelVersion(), st.ModelVersion)
			require.Equal(t, tt.reason, st.FallbackReason)
		})
	}
}
