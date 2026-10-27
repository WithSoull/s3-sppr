package migration

import (
	"context"
	"errors"
	"fmt"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/jackc/pgconn"
)

const uniqueViolation = "23505"

func (s *migrationService) SubmitActions(ctx context.Context, planID string, specs []model.ActionSpec) (model.SubmitResult, error) {
	planID, specs, err := normalizeSpecs(planID, specs)
	if err != nil {
		return model.SubmitResult{}, err
	}
	if id := findCycle(specs); id != "" {
		return model.SubmitResult{}, invalid("depends_on forms a cycle through action %s", id)
	}
	inRequest := make(map[string]struct{}, len(specs))
	ids := make([]string, 0, len(specs))
	var outside []string
	for _, spec := range specs {
		inRequest[spec.ActionID] = struct{}{}
		ids = append(ids, spec.ActionID)
	}
	for _, spec := range specs {
		for _, dep := range spec.DependsOn {
			if _, ok := inRequest[dep]; !ok {
				outside = append(outside, dep)
			}
		}
	}

	result := model.SubmitResult{Accepted: []string{}, Existing: []string{}}
	err = s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		result = model.SubmitResult{Accepted: []string{}, Existing: []string{}}
		known, err := s.actions.GetMany(ctx, append(ids, outside...))
		if err != nil {
			return err
		}
		var fresh []model.ActionSpec
		var unsuccessfulDeps []string
		for _, spec := range specs {
			if existing, ok := known[spec.ActionID]; ok {
				if !sameSpec(existing.ActionSpec, spec) {
					return fmt.Errorf("%w: action %s", model.ErrActionConflict, spec.ActionID)
				}
				result.Existing = append(result.Existing, spec.ActionID)
				continue
			}
			for _, dep := range spec.DependsOn {
				if _, ok := inRequest[dep]; ok {
					continue
				}
				accepted, ok := known[dep]
				if !ok || accepted.PlanID != planID {
					return invalid("action %s depends on %s, which is neither in the request nor accepted for plan %s", spec.ActionID, dep, planID)
				}
				if accepted.Status.Unsuccessful() {
					unsuccessfulDeps = append(unsuccessfulDeps, dep)
				}
			}
			fresh = append(fresh, spec)
			result.Accepted = append(result.Accepted, spec.ActionID)
		}
		if len(fresh) == 0 {
			return nil
		}
		if err := s.actions.Insert(ctx, fresh); err != nil {
			return err
		}
		if len(unsuccessfulDeps) == 0 {
			return nil
		}
		_, err = s.actions.CancelDependents(ctx, unsuccessfulDeps)
		return err
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return model.SubmitResult{}, fmt.Errorf("%w: a concurrent request accepted the same action_id; retry to compare", model.ErrActionConflict)
	}
	if err != nil {
		return model.SubmitResult{}, err
	}
	return result, nil
}
