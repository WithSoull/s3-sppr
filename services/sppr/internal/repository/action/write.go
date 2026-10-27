package action

import (
	"context"
	"errors"
	"fmt"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/contextx/txctx"
	"github.com/jackc/pgx/v4"
)

func requireTx(ctx context.Context, op string) error {
	if _, ok := txctx.ExtractTx(ctx); !ok {
		return fmt.Errorf("%s requires a transaction", op)
	}
	return nil
}

func (r *repo) Insert(ctx context.Context, specs []model.ActionSpec) error {
	if err := requireTx(ctx, "action insert"); err != nil {
		return err
	}
	for _, s := range specs {
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:insert", QueryRaw: `
			INSERT INTO migration_actions (action_id, plan_id, version_id, expected_generation,
			 source_class, target_class, size_bytes)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7)`},
			s.ActionID, s.PlanID, s.VersionID, s.ExpectedGeneration, string(s.SourceClass), string(s.TargetClass), s.SizeBytes); err != nil {
			return err
		}
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:insert_event", QueryRaw: `
			INSERT INTO migration_action_events (action_id, to_status) VALUES ($1::uuid, 'pending')`}, s.ActionID); err != nil {
			return err
		}
	}
	// Edges go last: a dependency may be submitted later in the same request.
	for _, s := range specs {
		if len(s.DependsOn) == 0 {
			continue
		}
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:insert_deps", QueryRaw: `
			INSERT INTO migration_action_deps (action_id, depends_on)
			SELECT $1::uuid, d FROM unnest($2::text[]::uuid[]) AS d`}, s.ActionID, s.DependsOn); err != nil {
			return err
		}
	}
	return nil
}

func (r *repo) Claim(ctx context.Context, maxBytes int64) (*model.Action, error) {
	if err := requireTx(ctx, "action claim"); err != nil {
		return nil, err
	}
	var actionID string
	err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "action:claim", QueryRaw: `
		WITH next AS (
			SELECT a.action_id FROM migration_actions a
			WHERE a.status = 'pending' AND a.size_bytes <= $1
			  AND NOT EXISTS (SELECT 1 FROM migration_action_deps d
			                  JOIN migration_actions p ON p.action_id = d.depends_on
			                  WHERE d.action_id = a.action_id
			                    AND (p.status <> 'succeeded' OR p.source_cleaned_at IS NULL))
			ORDER BY a.created_at, a.action_id
			LIMIT 1
			FOR UPDATE OF a SKIP LOCKED
		)
		UPDATE migration_actions a
		SET status = 'running', step = 'reserving', attempts = a.attempts + 1,
		    bytes_charged = a.bytes_charged + a.size_bytes, started_at = now(), updated_at = now()
		FROM next WHERE a.action_id = next.action_id
		RETURNING a.action_id::text`}, maxBytes).Scan(&actionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:claim_charge", QueryRaw: `
		INSERT INTO migration_budget_ledger (action_id, attempt, bytes)
		SELECT action_id, attempts, size_bytes FROM migration_actions WHERE action_id = $1::uuid`}, actionID); err != nil {
		return nil, err
	}
	if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:claim_event", QueryRaw: `
		INSERT INTO migration_action_events (action_id, from_status, to_status, to_step, bytes)
		SELECT action_id, 'pending', 'running', 'reserving', size_bytes
		FROM migration_actions WHERE action_id = $1::uuid`}, actionID); err != nil {
		return nil, err
	}
	return r.Get(ctx, actionID)
}

func (r *repo) Transition(ctx context.Context, t model.Transition) ([]string, error) {
	if err := requireTx(ctx, "action transition"); err != nil {
		return nil, err
	}
	tag, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:transition", QueryRaw: `
		UPDATE migration_actions
		SET status = $2, step = $3, reason = $4, updated_at = now(),
		    finished_at = CASE WHEN $2 IN ('succeeded','failed','cancelled') THEN now() END
		WHERE action_id = $1::uuid AND status = $5 AND COALESCE(step, '') = $6`},
		t.ActionID, string(t.Status), nullable(string(t.Step)), nullable(string(t.Reason)),
		string(t.FromStatus), string(t.FromStep))
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, model.ErrActionStateChanged
	}
	if t.Settle {
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:settle", QueryRaw: `
			UPDATE migration_budget_ledger SET bytes = $2, settled = true
			WHERE action_id = $1::uuid AND NOT settled`}, t.ActionID, t.Bytes); err != nil {
			return nil, err
		}
	}
	if t.Reopen {
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:reopen", QueryRaw: `
			INSERT INTO migration_budget_ledger (action_id, attempt, bytes)
			SELECT a.action_id,
			       COALESCE((SELECT max(attempt) FROM migration_budget_ledger WHERE action_id = a.action_id), 0) + 1,
			       a.size_bytes
			FROM migration_actions a WHERE a.action_id = $1::uuid`}, t.ActionID); err != nil {
			return nil, err
		}
	}
	if t.Settle || t.Reopen {
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:charged", QueryRaw: `
			UPDATE migration_actions a
			SET attempts = l.attempts, bytes_charged = l.bytes
			FROM (SELECT count(*) AS attempts, COALESCE(sum(bytes), 0) AS bytes
			      FROM migration_budget_ledger WHERE action_id = $1::uuid) l
			WHERE a.action_id = $1::uuid`}, t.ActionID); err != nil {
			return nil, err
		}
	}
	var bytes int64
	if t.Settle {
		bytes = t.Bytes
	}
	if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:transition_event", QueryRaw: `
		INSERT INTO migration_action_events (action_id, from_status, to_status, from_step, to_step, bytes, reason, error)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)`},
		t.ActionID, string(t.FromStatus), string(t.Status), nullable(string(t.FromStep)), nullable(string(t.Step)),
		bytes, nullable(string(t.Reason)), nullable(t.Error)); err != nil {
		return nil, err
	}
	if !t.Status.Unsuccessful() {
		return nil, nil
	}
	return r.CancelDependents(ctx, []string{t.ActionID})
}

func (r *repo) CancelPending(ctx context.Context, req model.CancelRequest) ([]string, error) {
	if err := requireTx(ctx, "action cancel"); err != nil {
		return nil, err
	}
	return r.scanIDs(ctx, db.Query{Name: "action:cancel", QueryRaw: `
		WITH cancelled AS (
			UPDATE migration_actions
			SET status = 'cancelled', reason = 'user', finished_at = now(), updated_at = now()
			WHERE status = 'pending'
			  AND (plan_id = $1::uuid OR action_id = ANY($2::text[]::uuid[]))
			RETURNING action_id
		), events AS (
			INSERT INTO migration_action_events (action_id, from_status, to_status, reason)
			SELECT action_id, 'pending', 'cancelled', 'user' FROM cancelled
		)
		SELECT action_id::text FROM cancelled ORDER BY action_id`}, nullable(req.PlanID), req.ActionIDs)
}

func (r *repo) CancelDependents(ctx context.Context, actionIDs []string) ([]string, error) {
	if err := requireTx(ctx, "dependent cancel"); err != nil {
		return nil, err
	}
	return r.scanIDs(ctx, db.Query{Name: "action:cancel_dependents", QueryRaw: `
		WITH RECURSIVE dependents(action_id) AS (
			SELECT d.action_id FROM migration_action_deps d WHERE d.depends_on = ANY($1::text[]::uuid[])
			UNION
			SELECT d.action_id FROM migration_action_deps d JOIN dependents x ON d.depends_on = x.action_id
		), cancelled AS (
			UPDATE migration_actions a
			SET status = 'cancelled', reason = 'dependency_failed', finished_at = now(), updated_at = now()
			FROM dependents x WHERE a.action_id = x.action_id AND a.status = 'pending'
			RETURNING a.action_id
		), events AS (
			INSERT INTO migration_action_events (action_id, from_status, to_status, reason)
			SELECT action_id, 'pending', 'cancelled', 'dependency_failed' FROM cancelled
		)
		SELECT action_id::text FROM cancelled ORDER BY action_id`}, actionIDs)
}

func (r *repo) scanIDs(ctx context.Context, q db.Query, args ...any) ([]string, error) {
	rows, err := r.db.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *repo) ListAwaitingSourceCleanup(ctx context.Context, limit int) ([]string, error) {
	var ids []string
	err := r.db.DB().ScanAllContext(ctx, &ids, db.Query{Name: "action:list_awaiting_source_cleanup", QueryRaw: `
		SELECT p.action_id::text FROM migration_actions p
		WHERE p.status = 'succeeded' AND p.source_cleaned_at IS NULL
		  AND EXISTS (SELECT 1 FROM migration_action_deps d
		              JOIN migration_actions a ON a.action_id = d.action_id
		              WHERE d.depends_on = p.action_id AND a.status = 'pending')
		ORDER BY p.finished_at, p.action_id
		LIMIT $1`}, limit)
	return ids, err
}

func (r *repo) MarkSourceCleaned(ctx context.Context, actionID string) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "action:mark_source_cleaned", QueryRaw: `
		UPDATE migration_actions SET source_cleaned_at = now()
		WHERE action_id = $1::uuid AND status = 'succeeded' AND source_cleaned_at IS NULL`}, actionID)
	return err
}
