package adminform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
)

type repo struct {
	db db.Client
	tx db.TxManager
}

func New(client db.Client, tx db.TxManager) service.FormCommands { return &repo{client, tx} }
func (r *repo) Execute(ctx context.Context, req model.CommandRequest, payload string, apply func(context.Context) error) (model.Command, error) {
	if _, err := uuid.Parse(req.CommandID); err != nil {
		return model.Command{}, fmt.Errorf("%w: command_id", model.ErrInvalidArgument)
	}
	var out model.Command
	err := r.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		// Serializes concurrent retries, including when the first has not inserted yet.
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "adminform:lock", QueryRaw: `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`}, req.CommandID); err != nil {
			return err
		}
		var oldPayload, oldUser string
		var raw []byte
		err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "adminform:replay", QueryRaw: `SELECT payload,initiator,command FROM admin_form_commands WHERE command_id=$1::uuid`}, req.CommandID).Scan(&oldPayload, &oldUser, &raw)
		if err == nil {
			if oldPayload != payload || oldUser != req.Initiator {
				return model.ErrCommandConflict
			}
			return json.Unmarshal(raw, &out)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		out = model.Command{CommandRequest: req, Status: model.CommandAccepted, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)}
		if err := apply(ctx); err != nil {
			switch {
			case errors.Is(err, model.ErrInvalidArgument):
				out.Code = "INVALID_ARGUMENT"
			case errors.Is(err, model.ErrPinExists):
				out.Code = "PIN_EXISTS"
			case errors.Is(err, model.ErrPinNotFound):
				out.Code = "PIN_NOT_FOUND"
			default:
				return err
			}
			out.Status = model.CommandRejected
			out.Message = err.Error()
		}
		raw, err = json.Marshal(out)
		if err != nil {
			return err
		}
		_, err = r.db.DB().ExecContext(ctx, db.Query{Name: "adminform:insert", QueryRaw: `INSERT INTO admin_form_commands(command_id,initiator,payload,command,created_at) VALUES($1::uuid,$2,$3,$4,$5)`}, req.CommandID, req.Initiator, payload, raw, out.CreatedAt)
		return err
	})
	return out, err
}
func (r *repo) Get(ctx context.Context, id string) (model.Command, error) {
	var raw []byte
	var out model.Command
	err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "adminform:get", QueryRaw: `SELECT command FROM admin_form_commands WHERE command_id=$1::uuid`}, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, model.ErrCommandNotFound
	}
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}
func (r *repo) List(ctx context.Context, token string) (model.CommandPage, error) {
	var at *time.Time
	var id string
	if token != "" {
		raw, tail, ok := strings.Cut(token, "|")
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if !ok || err != nil {
			return model.CommandPage{}, model.ErrInvalidArgument
		}
		if _, err := uuid.Parse(tail); err != nil {
			return model.CommandPage{}, model.ErrInvalidArgument
		}
		at = &parsed
		id = tail
	}
	rows, err := r.db.DB().QueryContext(ctx, db.Query{Name: "adminform:list", QueryRaw: `SELECT command FROM admin_form_commands WHERE ($1::timestamptz IS NULL OR (created_at,command_id)<($1,$2::uuid)) ORDER BY created_at DESC,command_id DESC LIMIT 51`}, at, nullable(id))
	if err != nil {
		return model.CommandPage{}, err
	}
	defer rows.Close()
	var out model.CommandPage
	for rows.Next() {
		var raw []byte
		var cmd model.Command
		if err := rows.Scan(&raw); err != nil {
			return out, err
		}
		if err := json.Unmarshal(raw, &cmd); err != nil {
			return out, err
		}
		out.Commands = append(out.Commands, cmd)
	}
	if len(out.Commands) > 50 {
		out.Commands = out.Commands[:50]
		last := out.Commands[49]
		out.NextPageToken = last.CreatedAt.Format(time.RFC3339Nano) + "|" + last.CommandID
	}
	return out, rows.Err()
}
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
