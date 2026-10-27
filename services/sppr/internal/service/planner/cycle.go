package planner

import (
	"context"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

func (s *plannerService) Run(ctx context.Context) error {
	if err := s.plans.FailInterrupted(ctx); err != nil {
		return fmt.Errorf("fail interrupted plans: %w", err)
	}
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	trigger := model.TriggerTimer
	var commands []string
	for {
		plan, err := s.Plan(ctx, trigger)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			logger.Error(ctx, "planning cycle failed", zap.Error(err))
		} else {
			logger.Info(ctx, "plan computed", zap.String("plan_id", plan.PlanID), zap.Int64("revision", plan.Revision),
				zap.String("status", string(plan.Status)), zap.Int("actions", plan.Summary.Actions))
		}
		if len(commands) > 0 && plan.PlanID != "" {
			if err := s.plans.AttachPlan(ctx, commands, plan.PlanID); err != nil {
				logger.Error(ctx, "attach plan to recompute commands", zap.Error(err))
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			trigger, commands = model.TriggerTimer, nil
		case id := <-s.recompute:
			trigger, commands = model.TriggerManual, []string{id}
			for len(s.recompute) > 0 {
				commands = append(commands, <-s.recompute)
			}
		}
	}
}

// Plan computes and stores one revision. A planning failure is stored as a
// failed plan as well as returned, so the administrator sees it.
func (s *plannerService) Plan(ctx context.Context, trigger model.PlanTrigger) (model.Plan, error) {
	settings, err := s.plans.GetSettings(ctx)
	if err != nil {
		return model.Plan{}, fmt.Errorf("read planner settings: %w", err)
	}
	params := paramsOf(settings, s.cfg.MaxRejectedSaved)
	plan, err := s.plans.CreatePlan(ctx, model.Plan{
		PlanID: s.newID(), Trigger: trigger, MethodVersion: s.predictor.MethodVersion(), Params: params,
	})
	if err != nil {
		return model.Plan{}, fmt.Errorf("create plan: %w", err)
	}
	var recs []model.Recommendation
	snap, planErr := s.snapshot(ctx, s.now().UTC(), params)
	switch {
	case planErr != nil:
		plan.Status, plan.Reason = model.PlanFailed, planErr.Error()
	case !snap.quality.Usable:
		plan.Status, plan.Reason = model.PlanHistoryIncomplete, snap.quality.Reason
	default:
		var result model.PlanResult
		result, planErr = Build(snap.input, s.newID)
		if planErr != nil {
			plan.Status, plan.Reason = model.PlanFailed, planErr.Error()
			break
		}
		plan.Status, plan.Summary = model.PlanReady, result.Summary
		recs = result.Recommendations
		for i := range recs {
			recs[i].PlanID = plan.PlanID
		}
	}
	// A cancelled cycle must still leave its row finished, not computing.
	finishCtx := context.WithoutCancel(ctx)
	if err := s.tx.ReadCommitted(finishCtx, func(ctx context.Context) error {
		return s.plans.FinishPlan(ctx, plan, recs)
	}); err != nil {
		return model.Plan{}, fmt.Errorf("store plan %s: %w", plan.PlanID, err)
	}
	if err := s.plans.PrunePlans(finishCtx, s.cfg.KeepPlans); err != nil {
		logger.Error(ctx, "prune old plans", zap.Error(err))
	}
	stored, err := s.plans.GetPlan(finishCtx, plan.PlanID, false)
	if err != nil {
		return model.Plan{}, err
	}
	return stored, planErr
}
