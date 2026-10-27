package action

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

func (r *repo) Get(ctx context.Context, actionID string) (*model.Action, error) {
	var rows []actionRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "action:get",
		QueryRaw: selectActions + ` WHERE a.action_id = $1::uuid`}, actionID); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, model.ErrActionNotFound
	}
	return rows[0].toModel(), nil
}

func (r *repo) GetMany(ctx context.Context, actionIDs []string) (map[string]*model.Action, error) {
	var rows []actionRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "action:get_many",
		QueryRaw: selectActions + ` WHERE a.action_id = ANY($1::text[]::uuid[])`}, actionIDs); err != nil {
		return nil, err
	}
	actions := make(map[string]*model.Action, len(rows))
	for _, row := range rows {
		actions[row.ActionID] = row.toModel()
	}
	return actions, nil
}

func (r *repo) List(ctx context.Context, f model.ActionFilter) ([]*model.Action, error) {
	var rows []actionRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "action:list", QueryRaw: selectActions + `
		WHERE ($1::uuid IS NULL OR a.plan_id = $1::uuid)
		  AND ($2::text IS NULL OR a.status = $2::text)
		  AND ($3::uuid IS NULL OR a.action_id > $3::uuid)
		ORDER BY a.action_id
		LIMIT $4`}, nullable(f.PlanID), nullable(string(f.Status)), nullable(f.PageToken), f.PageSize); err != nil {
		return nil, err
	}
	return toModels(rows), nil
}

func (r *repo) ListRunning(ctx context.Context) ([]*model.Action, error) {
	var rows []actionRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "action:list_running",
		QueryRaw: selectActions + ` WHERE a.status = 'running' ORDER BY a.started_at, a.action_id`}); err != nil {
		return nil, err
	}
	return toModels(rows), nil
}

type eventRow struct {
	At         time.Time `db:"at"`
	FromStatus string    `db:"from_status"`
	ToStatus   string    `db:"to_status"`
	FromStep   string    `db:"from_step"`
	ToStep     string    `db:"to_step"`
	Bytes      int64     `db:"bytes"`
	Reason     string    `db:"reason"`
	Error      string    `db:"error"`
}

func (r *repo) ListEvents(ctx context.Context, actionID string) ([]model.ActionEvent, error) {
	var rows []eventRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "action:events", QueryRaw: `
		SELECT at, COALESCE(from_status, '') AS from_status, to_status,
		       COALESCE(from_step, '') AS from_step, COALESCE(to_step, '') AS to_step,
		       bytes, COALESCE(reason, '') AS reason, COALESCE(error, '') AS error
		FROM migration_action_events WHERE action_id = $1::uuid ORDER BY id`}, actionID); err != nil {
		return nil, err
	}
	events := make([]model.ActionEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, model.ActionEvent{
			At:         row.At.UTC(),
			FromStatus: model.ActionStatus(row.FromStatus),
			ToStatus:   model.ActionStatus(row.ToStatus),
			FromStep:   model.ActionStep(row.FromStep),
			ToStep:     model.ActionStep(row.ToStep),
			Bytes:      row.Bytes,
			Reason:     model.Reason(row.Reason),
			Error:      row.Error,
		})
	}
	return events, nil
}
