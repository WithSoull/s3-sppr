package app

import (
	"context"
	"fmt"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	historyRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/history"
	planRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/plan"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	plannerService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
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
	s.plannerService = plannerService.NewService(plannerService.Config{
		Interval: cfg.Interval(), KeepPlans: cfg.KeepPlans(), MaxRejectedSaved: cfg.MaxRejectedSaved(),
	}, plans, historyRepository.NewRepository(pgClient), actions, tx, placements, migrations, s.FeatureBuilder(), ewma.NewPredictor())
	return s.plannerService, nil
}
