package migration

import (
	"fmt"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/google/uuid"
)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", model.ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// canonicalUUID accepts any UUID spelling and returns the form PostgreSQL prints.
func canonicalUUID(field, value string) (string, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return "", invalid("%s must be a UUID, got %q", field, value)
	}
	return id.String(), nil
}

// normalizeSpecs validates a submit request and returns it with canonical UUIDs.
func normalizeSpecs(planID string, specs []model.ActionSpec) (string, []model.ActionSpec, error) {
	planID, err := canonicalUUID("plan_id", planID)
	if err != nil {
		return "", nil, err
	}
	if len(specs) == 0 || len(specs) > maxActionsPerSubmit {
		return "", nil, invalid("actions must contain 1..%d items", maxActionsPerSubmit)
	}
	out := make([]model.ActionSpec, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))
	for i, s := range specs {
		field := func(name string) string { return fmt.Sprintf("actions[%d].%s", i, name) }
		if s.ActionID, err = canonicalUUID(field("action_id"), s.ActionID); err != nil {
			return "", nil, err
		}
		if _, ok := seen[s.ActionID]; ok {
			return "", nil, invalid("action_id %s is repeated", s.ActionID)
		}
		seen[s.ActionID] = struct{}{}
		if s.VersionID, err = canonicalUUID(field("version_id"), s.VersionID); err != nil {
			return "", nil, err
		}
		if s.ExpectedGeneration <= 0 {
			return "", nil, invalid("%s must be positive", field("expected_generation"))
		}
		if !s.SourceClass.Valid() || !s.TargetClass.Valid() {
			return "", nil, invalid("%s and %s must be HOT or COLD", field("source_class"), field("target_class"))
		}
		if s.SourceClass == s.TargetClass {
			return "", nil, invalid("%s must differ from source_class", field("target_class"))
		}
		if s.SizeBytes < 0 {
			return "", nil, invalid("%s must not be negative", field("size_bytes"))
		}
		deps := make([]string, 0, len(s.DependsOn))
		depSeen := map[string]struct{}{}
		for _, dep := range s.DependsOn {
			if dep, err = canonicalUUID(field("depends_on"), dep); err != nil {
				return "", nil, err
			}
			if dep == s.ActionID {
				return "", nil, invalid("action %s depends on itself", s.ActionID)
			}
			if _, ok := depSeen[dep]; ok {
				continue
			}
			depSeen[dep] = struct{}{}
			deps = append(deps, dep)
		}
		s.DependsOn = deps
		s.PlanID = planID
		out = append(out, s)
	}
	return planID, out, nil
}

// findCycle returns an action on a dependency cycle among specs, if any.
func findCycle(specs []model.ActionSpec) string {
	deps := make(map[string][]string, len(specs))
	for _, s := range specs {
		deps[s.ActionID] = s.DependsOn
	}
	const (
		visiting = 1
		done     = 2
	)
	state := map[string]int{}
	var visit func(string) string
	visit = func(id string) string {
		switch state[id] {
		case visiting:
			return id
		case done:
			return ""
		}
		state[id] = visiting
		for _, dep := range deps[id] {
			if _, inRequest := deps[dep]; inRequest {
				if found := visit(dep); found != "" {
					return found
				}
			}
		}
		state[id] = done
		return ""
	}
	for _, s := range specs {
		if found := visit(s.ActionID); found != "" {
			return found
		}
	}
	return ""
}

func sameSpec(a, b model.ActionSpec) bool {
	if a.PlanID != b.PlanID || a.VersionID != b.VersionID || a.ExpectedGeneration != b.ExpectedGeneration ||
		a.SourceClass != b.SourceClass || a.TargetClass != b.TargetClass || a.SizeBytes != b.SizeBytes ||
		len(a.DependsOn) != len(b.DependsOn) {
		return false
	}
	set := make(map[string]struct{}, len(a.DependsOn))
	for _, dep := range a.DependsOn {
		set[dep] = struct{}{}
	}
	for _, dep := range b.DependsOn {
		if _, ok := set[dep]; !ok {
			return false
		}
	}
	return true
}
