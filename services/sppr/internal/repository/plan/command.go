package plan

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

// commandRequest is the stored form of what the administrator asked for; a
// retry with the same command_id must ask for exactly the same.
type commandRequest struct {
	RecommendationIDs []string      `json:"recommendation_ids,omitempty"`
	Settings          *settingsJSON `json:"settings,omitempty"`
}

type settingsJSON struct {
	HorizonSeconds  int64   `json:"horizon_seconds"`
	HotFixedMs      float64 `json:"hot_fixed_ms"`
	HotMiBPerMs     float64 `json:"hot_mib_per_ms"`
	ColdFixedMs     float64 `json:"cold_fixed_ms"`
	ColdMiBPerMs    float64 `json:"cold_mib_per_ms"`
	PenaltyMsPerMiB float64 `json:"penalty_ms_per_mib"`
	MinGainMs       float64 `json:"min_gain_ms"`
	HotUtilization  float64 `json:"hot_utilization"`
	ColdUtilization float64 `json:"cold_utilization"`
}

type commandResult struct {
	Missing []string `json:"missing,omitempty"`
	Actions []string `json:"actions,omitempty"`
}

const selectCommands = `
SELECT command_id::text, type, initiator, COALESCE(plan_id::text, '') AS plan_id, request, status, code, message,
       result, created_at
FROM admin_commands`

type commandRow struct {
	CommandID string    `db:"command_id"`
	Type      string    `db:"type"`
	Initiator string    `db:"initiator"`
	PlanID    string    `db:"plan_id"`
	Request   []byte    `db:"request"`
	Status    string    `db:"status"`
	Code      string    `db:"code"`
	Message   string    `db:"message"`
	Result    []byte    `db:"result"`
	CreatedAt time.Time `db:"created_at"`
}

func (row commandRow) toModel() (model.Command, error) {
	var req commandRequest
	if err := json.Unmarshal(row.Request, &req); err != nil {
		return model.Command{}, fmt.Errorf("decode command %s: %w", row.CommandID, err)
	}
	var res commandResult
	if err := json.Unmarshal(row.Result, &res); err != nil {
		return model.Command{}, fmt.Errorf("decode command %s result: %w", row.CommandID, err)
	}
	cmd := model.Command{
		CommandRequest: model.CommandRequest{
			CommandID: row.CommandID, Initiator: row.Initiator, Type: model.CommandType(row.Type),
			PlanID: row.PlanID, RecommendationIDs: req.RecommendationIDs,
		},
		Status: model.CommandStatus(row.Status), Code: row.Code, Message: row.Message,
		Missing: res.Missing, Actions: res.Actions, CreatedAt: row.CreatedAt.UTC(),
	}
	if s := req.Settings; s != nil {
		cmd.Settings = &model.PlannerSettings{
			Horizon:    time.Duration(s.HorizonSeconds) * time.Second,
			HotFixedMs: s.HotFixedMs, HotMiBPerMs: s.HotMiBPerMs, ColdFixedMs: s.ColdFixedMs, ColdMiBPerMs: s.ColdMiBPerMs,
			PenaltyMsPerMiB: s.PenaltyMsPerMiB, MinGainMs: s.MinGainMs,
			HotUtilization: s.HotUtilization, ColdUtilization: s.ColdUtilization,
		}
	}
	return cmd, nil
}

func (r *repo) GetCommand(ctx context.Context, commandID string) (model.Command, error) {
	var rows []commandRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "command:get",
		QueryRaw: selectCommands + ` WHERE command_id = $1::uuid`}, commandID); err != nil {
		return model.Command{}, err
	}
	if len(rows) == 0 {
		return model.Command{}, model.ErrCommandNotFound
	}
	return rows[0].toModel()
}

func (r *repo) InsertCommand(ctx context.Context, cmd model.Command) error {
	req := commandRequest{RecommendationIDs: cmd.RecommendationIDs}
	if s := cmd.Settings; s != nil {
		req.Settings = &settingsJSON{
			HorizonSeconds: int64(s.Horizon / time.Second),
			HotFixedMs:     s.HotFixedMs, HotMiBPerMs: s.HotMiBPerMs, ColdFixedMs: s.ColdFixedMs, ColdMiBPerMs: s.ColdMiBPerMs,
			PenaltyMsPerMiB: s.PenaltyMsPerMiB, MinGainMs: s.MinGainMs,
			HotUtilization: s.HotUtilization, ColdUtilization: s.ColdUtilization,
		}
	}
	request, err := json.Marshal(req)
	if err != nil {
		return err
	}
	result, err := json.Marshal(commandResult{Missing: cmd.Missing, Actions: cmd.Actions})
	if err != nil {
		return err
	}
	_, err = r.db.DB().ExecContext(ctx, db.Query{Name: "command:insert", QueryRaw: `
		INSERT INTO admin_commands (command_id, type, initiator, plan_id, request, status, code, message, result)
		VALUES ($1::uuid, $2, $3, $4::uuid, $5, $6, $7, $8, $9)`},
		cmd.CommandID, string(cmd.Type), cmd.Initiator, nullable(cmd.PlanID), request,
		string(cmd.Status), cmd.Code, cmd.Message, result)
	return err
}

// Commands are listed newest first; the page token is "<created_at>|<command_id>"
// of the last command of the previous page.
func (r *repo) ListCommands(ctx context.Context, f model.CommandFilter) ([]model.Command, error) {
	var beforeAt *time.Time
	beforeID := ""
	if f.PageToken != "" {
		at, id, ok := strings.Cut(f.PageToken, "|")
		t, err := time.Parse(time.RFC3339Nano, at)
		if !ok || err != nil {
			return nil, fmt.Errorf("%w: page_token", model.ErrInvalidArgument)
		}
		beforeAt, beforeID = &t, id
	}
	var rows []commandRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "command:list", QueryRaw: selectCommands + `
		WHERE ($1::uuid IS NULL OR plan_id = $1::uuid)
		  AND ($2::timestamptz IS NULL OR (created_at, command_id) < ($2, $3::uuid))
		ORDER BY created_at DESC, command_id DESC LIMIT $4`},
		nullable(f.PlanID), beforeAt, nullable(beforeID), f.PageSize); err != nil {
		return nil, err
	}
	out := make([]model.Command, 0, len(rows))
	for _, row := range rows {
		cmd, err := row.toModel()
		if err != nil {
			return nil, err
		}
		out = append(out, cmd)
	}
	return out, nil
}

func (r *repo) AttachPlan(ctx context.Context, commandIDs []string, planID string) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "command:attach_plan", QueryRaw: `
		UPDATE admin_commands SET plan_id = $2::uuid WHERE command_id = ANY($1::text[]::uuid[])`}, commandIDs, planID)
	return err
}
