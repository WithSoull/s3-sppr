package planner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
)

const uniqueViolation = "23505"

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", model.ErrInvalidArgument, fmt.Sprintf(format, args...))
}

func (s *plannerService) Execute(ctx context.Context, req model.CommandRequest) (model.Command, error) {
	req, err := normalize(req)
	if err != nil {
		return model.Command{}, err
	}
	if existing, err := s.plans.GetCommand(ctx, req.CommandID); err == nil {
		return replay(existing, req)
	} else if !errors.Is(err, model.ErrCommandNotFound) {
		return model.Command{}, err
	}
	var cmd model.Command
	switch req.Type {
	case model.CommandConfirm:
		cmd, err = s.confirm(ctx, req)
	case model.CommandReject:
		cmd, err = s.reject(ctx, req)
	case model.CommandCancelPending:
		cmd, err = s.cancelPending(ctx, req)
	case model.CommandPause, model.CommandResume:
		cmd, err = s.record(ctx, req, func(ctx context.Context, cmd *model.Command) error {
			return s.migrations.SetPaused(ctx, req.Type == model.CommandPause)
		})
	case model.CommandSettings:
		cmd, err = s.record(ctx, req, func(ctx context.Context, cmd *model.Command) error {
			return s.plans.UpdateSettings(ctx, *req.Settings)
		})
	case model.CommandRecompute:
		cmd, err = s.record(ctx, req, func(context.Context, *model.Command) error { return nil })
		if err == nil {
			select {
			case s.recompute <- req.CommandID:
			default:
				// The queue is full, so a cycle is already due and will see this request's state.
			}
		}
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		// A concurrent request with the same command_id won; answer as a retry.
		existing, getErr := s.plans.GetCommand(ctx, req.CommandID)
		if getErr != nil {
			return model.Command{}, getErr
		}
		return replay(existing, req)
	}
	return cmd, err
}

func normalize(req model.CommandRequest) (model.CommandRequest, error) {
	if _, err := uuid.Parse(req.CommandID); err != nil {
		return req, invalid("command_id must be a UUID")
	}
	if req.Initiator == "" {
		return req, invalid("initiator is required")
	}
	switch req.Type {
	case model.CommandConfirm, model.CommandReject:
		if _, err := uuid.Parse(req.PlanID); err != nil {
			return req, invalid("plan_id must be a UUID")
		}
	case model.CommandCancelPending:
		if (req.PlanID == "") == (len(req.RecommendationIDs) == 0) {
			return req, invalid("cancel_pending takes either plan_id or action IDs")
		}
	case model.CommandSettings:
		if req.Settings == nil {
			return req, invalid("settings are required")
		}
		if err := validateSettings(*req.Settings); err != nil {
			return req, err
		}
	case model.CommandPause, model.CommandResume, model.CommandRecompute:
	default:
		return req, invalid("unknown command type %q", req.Type)
	}
	if req.Type != model.CommandConfirm && req.Type != model.CommandCancelPending && len(req.RecommendationIDs) > 0 {
		return req, invalid("%s takes no recommendation IDs", req.Type)
	}
	for _, id := range req.RecommendationIDs {
		if _, err := uuid.Parse(id); err != nil {
			return req, invalid("recommendation ID %q is not a UUID", id)
		}
	}
	ids := slices.Clone(req.RecommendationIDs)
	slices.Sort(ids)
	req.RecommendationIDs = slices.Compact(ids)
	if len(req.RecommendationIDs) == 0 {
		req.RecommendationIDs = nil
	}
	return req, nil
}

func validateSettings(s model.PlannerSettings) error {
	_, err := Build(model.PlanInput{Params: paramsOf(s, 0)}, func() string { return "" })
	if err != nil {
		return invalid("%v", err)
	}
	if s.Horizon%time.Second != 0 {
		return invalid("horizon must be whole seconds")
	}
	return nil
}

// replay answers a retried command with its first result; a different command
// under the same ID is a client error.
func replay(existing model.Command, req model.CommandRequest) (model.Command, error) {
	same := existing.Type == req.Type && existing.PlanID == req.PlanID &&
		slices.Equal(existing.RecommendationIDs, req.RecommendationIDs) &&
		(existing.Settings == nil) == (req.Settings == nil) &&
		(existing.Settings == nil || *existing.Settings == *req.Settings)
	if !same {
		return model.Command{}, fmt.Errorf("%w: command %s", model.ErrCommandConflict, req.CommandID)
	}
	return existing, nil
}

// record applies an unconditional command and stores it in one transaction.
func (s *plannerService) record(ctx context.Context, req model.CommandRequest, apply func(context.Context, *model.Command) error) (model.Command, error) {
	cmd := model.Command{CommandRequest: req, Status: model.CommandAccepted, CreatedAt: s.now().UTC()}
	err := s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		if err := apply(ctx, &cmd); err != nil {
			return err
		}
		return s.plans.InsertCommand(ctx, cmd)
	})
	return cmd, err
}

func refuse(cmd *model.Command, code error, message string) {
	cmd.Status, cmd.Code, cmd.Message = model.CommandRejected, code.Error(), message
}

// confirm submits the chosen recommendations to the executor. Only the latest
// revision can be confirmed, and a chosen action must come with every action it
// depends on: the plan is never completed automatically.
func (s *plannerService) confirm(ctx context.Context, req model.CommandRequest) (model.Command, error) {
	cmd := model.Command{CommandRequest: req, Status: model.CommandAccepted, CreatedAt: s.now().UTC()}
	err := s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		plan, stale, err := s.lockLatest(ctx, req.PlanID, model.PlanReady, model.PlanConfirmed)
		if err != nil {
			return err
		}
		if stale != "" {
			refuse(&cmd, model.ErrPlanStale, stale)
			return s.plans.InsertCommand(ctx, cmd)
		}
		recs, err := s.plans.SelectedRecommendations(ctx, plan.PlanID)
		if err != nil {
			return err
		}
		chosen, err := choose(recs, req.RecommendationIDs)
		if err != nil {
			return err
		}
		missing, err := s.unsubmitted(ctx, plan.PlanID, missingDependencies(chosen))
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			refuse(&cmd, model.ErrDependencyRequired, "the selection needs the actions that free space for it")
			cmd.Missing = missing
			return s.plans.InsertCommand(ctx, cmd)
		}
		specs := make([]model.ActionSpec, 0, len(chosen))
		for _, r := range chosen {
			specs = append(specs, model.ActionSpec{
				ActionID: r.RecommendationID, PlanID: plan.PlanID, VersionID: r.VersionID,
				ExpectedGeneration: r.ExpectedGeneration, SourceClass: r.SourceClass, TargetClass: r.TargetClass,
				SizeBytes: r.SizeBytes, DependsOn: r.DependsOn,
			})
			cmd.Actions = append(cmd.Actions, r.RecommendationID)
		}
		if len(specs) > 0 {
			if _, err := s.migrations.SubmitActions(ctx, plan.PlanID, specs); err != nil {
				return err
			}
		}
		if err := s.plans.SetPlanStatus(ctx, plan.PlanID, model.PlanConfirmed); err != nil {
			return err
		}
		return s.plans.InsertCommand(ctx, cmd)
	})
	return cmd, err
}

func (s *plannerService) reject(ctx context.Context, req model.CommandRequest) (model.Command, error) {
	cmd := model.Command{CommandRequest: req, Status: model.CommandAccepted, CreatedAt: s.now().UTC()}
	err := s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		plan, stale, err := s.lockLatest(ctx, req.PlanID, model.PlanReady)
		if err != nil {
			return err
		}
		if stale != "" {
			refuse(&cmd, model.ErrPlanStale, stale)
			return s.plans.InsertCommand(ctx, cmd)
		}
		if err := s.plans.SetPlanStatus(ctx, plan.PlanID, model.PlanRejected); err != nil {
			return err
		}
		return s.plans.InsertCommand(ctx, cmd)
	})
	return cmd, err
}

// lockLatest locks the plan and explains why it is stale: not the newest
// finished revision, or in a status the command does not accept.
func (s *plannerService) lockLatest(ctx context.Context, planID string, allowed ...model.PlanStatus) (model.Plan, string, error) {
	plan, err := s.plans.GetPlan(ctx, planID, true)
	if err != nil {
		return model.Plan{}, "", err
	}
	latest, err := s.plans.LatestPlan(ctx)
	if err != nil {
		return model.Plan{}, "", err
	}
	if latest != nil && latest.PlanID != plan.PlanID {
		return plan, fmt.Sprintf("revision %d replaced plan revision %d; compute a new plan and review it", latest.Revision, plan.Revision), nil
	}
	if !slices.Contains(allowed, plan.Status) {
		return plan, fmt.Sprintf("plan is %s", plan.Status), nil
	}
	return plan, "", nil
}

func choose(recs []model.Recommendation, ids []string) ([]model.Recommendation, error) {
	if len(ids) == 0 {
		return recs, nil
	}
	byID := make(map[string]model.Recommendation, len(recs))
	for _, r := range recs {
		byID[r.RecommendationID] = r
	}
	chosen := make([]model.Recommendation, 0, len(ids))
	for _, id := range ids {
		r, ok := byID[id]
		if !ok {
			return nil, invalid("recommendation %s is not selected in this plan", id)
		}
		chosen = append(chosen, r)
	}
	// Keep plan order so dependencies are listed before their dependents.
	slices.SortFunc(chosen, func(a, b model.Recommendation) int { return a.Order - b.Order })
	return chosen, nil
}

func missingDependencies(chosen []model.Recommendation) []string {
	in := make(map[string]bool, len(chosen))
	for _, r := range chosen {
		in[r.RecommendationID] = true
	}
	var missing []string
	for _, r := range chosen {
		for _, d := range r.DependsOn {
			if !in[d] && !slices.Contains(missing, d) {
				missing = append(missing, d)
			}
		}
	}
	slices.Sort(missing)
	return missing
}

// unsubmitted drops dependencies an earlier confirm of the same plan already
// handed to the executor.
func (s *plannerService) unsubmitted(ctx context.Context, planID string, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	accepted, err := s.actions.GetMany(ctx, ids)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, id := range ids {
		if a, ok := accepted[id]; !ok || a.PlanID != planID {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *plannerService) cancelPending(ctx context.Context, req model.CommandRequest) (model.Command, error) {
	return s.record(ctx, req, func(ctx context.Context, cmd *model.Command) error {
		result, err := s.migrations.CancelActions(ctx, model.CancelRequest{PlanID: req.PlanID, ActionIDs: req.RecommendationIDs})
		if err != nil {
			return err
		}
		cmd.Actions = append(append(cmd.Actions, result.Cancelled...), result.Dependents...)
		return nil
	})
}
