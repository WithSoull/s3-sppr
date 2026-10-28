package app

import (
	"context"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	historyRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/history"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository/modelversion"
	planRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/plan"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/modelmanager"
	plannerService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
)

// PlannerService also stores the env defaults of the planner settings on the
// first start; afterwards the administrator's values are kept.
func (s *serviceProvider) PlannerService(ctx context.Context) (service.PlannerService, error) {
	if s.plannerService != nil {
		return s.plannerService, nil
	}
	pgClient, err := s.PGClient(ctx)
	if err != nil {
		return nil, err
	}
	cfg := s.cfg.Planner
	plans := planRepository.NewRepository(pgClient)
	if err := plans.EnsureSettings(ctx, model.PlannerSettings{
		Horizon:    cfg.Horizon(),
		HotFixedMs: cfg.HotFixedMs(), HotMiBPerMs: cfg.HotMiBPerMs(),
		ColdFixedMs: cfg.ColdFixedMs(), ColdMiBPerMs: cfg.ColdMiBPerMs(),
		PenaltyMsPerMiB: cfg.PenaltyMsPerMiB(), MinGainMs: cfg.MinGainMs(),
		HotUtilization: cfg.HotUtilization(), ColdUtilization: cfg.ColdUtilization(),
	}); err != nil {
		return nil, fmt.Errorf("store default planner settings: %w", err)
	}
	actions, err := s.ActionRepository(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.TxManager(ctx)
	if err != nil {
		return nil, err
	}
	placements, err := s.PlacementClient()
	if err != nil {
		return nil, err
	}
	migrations, err := s.MigrationService(ctx)
	if err != nil {
		return nil, err
	}
	settings, err := plans.GetSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("read planner horizon: %w", err)
	}
	manager := modelmanager.New(modelmanager.Config{
		CurrentHorizon: func(ctx context.Context) (time.Duration, error) {
			settings, err := plans.GetSettings(ctx)
			return settings.Horizon, err
		},
		Registry: cfg.ModelRegistry(), Bootstrap: cfg.ModelDir(), Horizon: settings.Horizon,
		MaxAge: cfg.ModelMaxAge(), Every: cfg.ModelUpdateEvery(), MinHistory: cfg.ModelMinHistory(),
		Window: cfg.ModelTrainWindow(), Timeout: cfg.ModelTrainTimeout(), Command: cfg.TrainCommand(),
	}, modelversion.New(pgClient, tx), historyRepository.NewRepository(pgClient), placements)
	if err := manager.Restore(ctx); err != nil {
		return nil, fmt.Errorf("restore model registry: %w", err)
	}
	predictor := ewma.NewPredictor()
	s.plannerService = plannerService.NewService(plannerService.Config{
		Models: manager, MaxHistoryLag: cfg.MaxHistoryLag(), Interval: cfg.Interval(), KeepPlans: cfg.KeepPlans(), MaxRejectedSaved: cfg.MaxRejectedSaved(),
	}, plans, historyRepository.NewRepository(pgClient), actions, tx, placements, migrations, s.FeatureBuilder(), predictor)
	return s.plannerService, nil
}

func plannerPredictor(cfg config.PlannerConfig, storedHorizon time.Duration) (service.Predictor, string) {
	if cfg.Predictor() == "ewma" {
		return ewma.NewPredictor(), ""
	}
	m, err := lgbm.Load(cfg.ModelDir(), cfg.Horizon())
	if err == nil && storedHorizon != cfg.Horizon() {
		err = fmt.Errorf("stored planner horizon %s differs from PLANNER_HORIZON %s", storedHorizon, cfg.Horizon())
	}
	if err != nil {
		return ewma.NewPredictor(), err.Error()
	}
	return m, ""
}
