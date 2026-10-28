package app

import (
	"context"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	historyRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/history"
	planRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/plan"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	plannerService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/baseline"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
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
	predictor, fallback := plannerPredictor(cfg, settings.Horizon)
	if fallback != "" {
		logger.Warn(ctx, "planner model load failed; using EWMA", zap.String("fallback_reason", fallback))
	}
	s.plannerService = plannerService.NewService(plannerService.Config{
		FallbackReason: fallback, Interval: cfg.Interval(), KeepPlans: cfg.KeepPlans(), MaxRejectedSaved: cfg.MaxRejectedSaved(),
	}, plans, historyRepository.NewRepository(pgClient), actions, tx, placements, migrations, s.FeatureBuilder(), predictor)
	return s.plannerService, nil
}

func plannerPredictor(cfg config.PlannerConfig, storedHorizon time.Duration) (service.Predictor, string) {
	if cfg.Predictor() == "ewma" {
		return ewma.NewPredictor(), ""
	}
	if kind := cfg.Predictor(); kind == "age" || kind == "recency" || kind == "frequency" {
		var mapping baseline.Map
		var err error
		if kind != "frequency" {
			mapping, err = baseline.Load(cfg.BaselineMap())
		}
		if err == nil {
			var p *baseline.Predictor
			p, err = baseline.New(kind, mapping)
			if err == nil {
				return p, ""
			}
		}
		return ewma.NewPredictor(), err.Error()
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
