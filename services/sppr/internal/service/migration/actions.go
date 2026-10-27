package migration

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

func (s *migrationService) ListActions(ctx context.Context, f model.ActionFilter) (model.ActionPage, error) {
	var err error
	if f.PlanID != "" {
		if f.PlanID, err = canonicalUUID("plan_id", f.PlanID); err != nil {
			return model.ActionPage{}, err
		}
	}
	if f.PageToken != "" {
		if f.PageToken, err = canonicalUUID("page_token", f.PageToken); err != nil {
			return model.ActionPage{}, err
		}
	}
	if f.Status != "" && !f.Status.Valid() {
		return model.ActionPage{}, invalid("unknown status %q", f.Status)
	}
	switch {
	case f.PageSize < 0 || f.PageSize > maxPageSize:
		return model.ActionPage{}, invalid("page_size must be within 1..%d", maxPageSize)
	case f.PageSize == 0:
		f.PageSize = defaultPageSize
	}
	pageSize := f.PageSize
	f.PageSize++
	actions, err := s.actions.List(ctx, f)
	if err != nil {
		return model.ActionPage{}, err
	}
	page := model.ActionPage{Actions: []model.ActionView{}}
	if len(actions) > pageSize {
		actions = actions[:pageSize]
		page.NextPageToken = actions[pageSize-1].ActionID
	}
	gate, err := s.blockGate(ctx, actions)
	if err != nil {
		return model.ActionPage{}, err
	}
	for _, a := range actions {
		page.Actions = append(page.Actions, model.ActionView{Action: a, BlockedBy: gate.blockedBy(a)})
	}
	return page, nil
}

func (s *migrationService) GetAction(ctx context.Context, actionID string) (model.ActionDetails, error) {
	actionID, err := canonicalUUID("action_id", actionID)
	if err != nil {
		return model.ActionDetails{}, err
	}
	a, err := s.actions.Get(ctx, actionID)
	if err != nil {
		return model.ActionDetails{}, err
	}
	events, err := s.actions.ListEvents(ctx, actionID)
	if err != nil {
		return model.ActionDetails{}, err
	}
	gate, err := s.blockGate(ctx, []*model.Action{a})
	if err != nil {
		return model.ActionDetails{}, err
	}
	return model.ActionDetails{Action: a, BlockedBy: gate.blockedBy(a), Events: events}, nil
}

func (s *migrationService) CancelActions(ctx context.Context, req model.CancelRequest) (model.CancelResult, error) {
	if (req.PlanID == "") == (len(req.ActionIDs) == 0) {
		return model.CancelResult{}, invalid("set either plan_id or action_ids")
	}
	var err error
	if req.PlanID != "" {
		if req.PlanID, err = canonicalUUID("plan_id", req.PlanID); err != nil {
			return model.CancelResult{}, err
		}
	}
	ids := make([]string, 0, len(req.ActionIDs))
	for _, id := range req.ActionIDs {
		if id, err = canonicalUUID("action_ids", id); err != nil {
			return model.CancelResult{}, err
		}
		ids = append(ids, id)
	}
	req.ActionIDs = ids
	var result model.CancelResult
	err = s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		cancelled, err := s.actions.CancelPending(ctx, req)
		if err != nil {
			return err
		}
		dependents := []string{}
		if len(cancelled) > 0 {
			if dependents, err = s.actions.CancelDependents(ctx, cancelled); err != nil {
				return err
			}
		}
		result = model.CancelResult{Cancelled: cancelled, Dependents: dependents}
		return nil
	})
	if err == nil && s.observer != nil {
		if len(result.Cancelled) > 0 {
			s.observer.Actions(ctx, string(model.ActionCancelled), string(model.ReasonUser), int64(len(result.Cancelled)))
		}
		if len(result.Dependents) > 0 {
			s.observer.Actions(ctx, string(model.ActionCancelled), string(model.ReasonDependencyFailed), int64(len(result.Dependents)))
		}
	}
	return result, err
}

// gate holds what decides why a pending action waits.
type gate struct {
	paused    bool
	remaining int64
}

func (s *migrationService) blockGate(ctx context.Context, actions []*model.Action) (gate, error) {
	pending := false
	for _, a := range actions {
		pending = pending || a.Status == model.ActionPending
	}
	if !pending {
		return gate{}, nil
	}
	budget, err := s.GetBudget(ctx)
	if err != nil {
		return gate{}, err
	}
	settings, err := s.settings.Get(ctx)
	if err != nil {
		return gate{}, err
	}
	return gate{paused: settings.Paused, remaining: budget.Remaining}, nil
}

func (g gate) blockedBy(a *model.Action) model.BlockedBy {
	switch {
	case a.Status != model.ActionPending:
		return model.BlockedByNone
	case g.paused:
		return model.BlockedByPaused
	case a.DepsPending:
		return model.BlockedByDependencies
	case a.SizeBytes > g.remaining:
		return model.BlockedByBudget
	}
	return model.BlockedByNone
}
