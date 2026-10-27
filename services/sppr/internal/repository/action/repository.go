package action

import (
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

type repo struct{ db db.Client }

func NewRepository(client db.Client) repository.ActionRepository { return &repo{db: client} }

const selectActions = `
SELECT a.action_id::text, a.plan_id::text, a.version_id::text, a.expected_generation,
       a.source_class, a.target_class, a.size_bytes, a.status,
       COALESCE(a.step, '') AS step, COALESCE(a.reason, '') AS reason,
       a.attempts, a.bytes_charged, a.created_at, a.started_at, a.finished_at, a.updated_at,
       EXISTS (SELECT 1 FROM migration_budget_ledger l
               WHERE l.action_id = a.action_id AND NOT l.settled) AS open_charge,
       EXISTS (SELECT 1 FROM migration_action_deps d
               JOIN migration_actions p ON p.action_id = d.depends_on
               WHERE d.action_id = a.action_id
                 AND (p.status <> 'succeeded' OR p.source_cleaned_at IS NULL)) AS deps_pending,
       ARRAY(SELECT d.depends_on::text FROM migration_action_deps d
             WHERE d.action_id = a.action_id ORDER BY d.depends_on) AS depends_on
FROM migration_actions a`

type actionRow struct {
	ActionID           string     `db:"action_id"`
	PlanID             string     `db:"plan_id"`
	VersionID          string     `db:"version_id"`
	ExpectedGeneration int64      `db:"expected_generation"`
	SourceClass        string     `db:"source_class"`
	TargetClass        string     `db:"target_class"`
	SizeBytes          int64      `db:"size_bytes"`
	Status             string     `db:"status"`
	Step               string     `db:"step"`
	Reason             string     `db:"reason"`
	Attempts           int32      `db:"attempts"`
	BytesCharged       int64      `db:"bytes_charged"`
	CreatedAt          time.Time  `db:"created_at"`
	StartedAt          *time.Time `db:"started_at"`
	FinishedAt         *time.Time `db:"finished_at"`
	UpdatedAt          time.Time  `db:"updated_at"`
	OpenCharge         bool       `db:"open_charge"`
	DepsPending        bool       `db:"deps_pending"`
	DependsOn          []string   `db:"depends_on"`
}

func (r actionRow) toModel() *model.Action {
	return &model.Action{
		ActionSpec: model.ActionSpec{
			ActionID:           r.ActionID,
			PlanID:             r.PlanID,
			VersionID:          r.VersionID,
			ExpectedGeneration: r.ExpectedGeneration,
			SourceClass:        model.StorageClass(r.SourceClass),
			TargetClass:        model.StorageClass(r.TargetClass),
			SizeBytes:          r.SizeBytes,
			DependsOn:          r.DependsOn,
		},
		Status:       model.ActionStatus(r.Status),
		Step:         model.ActionStep(r.Step),
		Reason:       model.Reason(r.Reason),
		Attempts:     r.Attempts,
		BytesCharged: r.BytesCharged,
		OpenCharge:   r.OpenCharge,
		DepsPending:  r.DepsPending,
		CreatedAt:    r.CreatedAt.UTC(),
		StartedAt:    utc(r.StartedAt),
		FinishedAt:   utc(r.FinishedAt),
		UpdatedAt:    r.UpdatedAt.UTC(),
	}
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func toModels(rows []actionRow) []*model.Action {
	actions := make([]*model.Action, 0, len(rows))
	for _, row := range rows {
		actions = append(actions, row.toModel())
	}
	return actions
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
