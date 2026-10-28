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
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/baseline"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
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
	var models service.ModelLifecycle
	predictor, fallback := service.Predictor(ewma.NewPredictor()), ""
	if cfg.Predictor() == "lgbm" {
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
		models = manager
	} else {
		predictor, fallback = fixedPredictor(cfg)
		if fallback != "" {
			logger.Warn(ctx, "planner baseline load failed; using EWMA", zap.String("fallback_reason", fallback))
		}
	}
	s.plannerService = plannerService.NewService(plannerService.Config{
		Models: models, MaxHistoryLag: cfg.MaxHistoryLag(), FallbackReason: fallback, Interval: cfg.Interval(), KeepPlans: cfg.KeepPlans(), MaxRejectedSaved: cfg.MaxRejectedSaved(),
	}, plans, historyRepository.NewRepository(pgClient), actions, tx, placements, migrations, s.FeatureBuilder(), predictor)
	return s.plannerService, nil
}

// fixedPredictor serves the experiment's comparison policies: one predictor
// for the whole run, without the model lifecycle.
func fixedPredictor(cfg config.PlannerConfig) (service.Predictor, string) {
	kind := cfg.Predictor()
	if kind == "ewma" {
		return ewma.NewPredictor(), ""
	}
	var mapping baseline.Map
	var err error
	if kind != "frequency" {
		mapping, err = baseline.Load(cfg.BaselineMap())
	}
	if err == nil {
		var p *baseline.Predictor
		if p, err = baseline.New(kind, mapping); err == nil {
			return p, ""
		}
	}
	return ewma.NewPredictor(), err.Error()
}
