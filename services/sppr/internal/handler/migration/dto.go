package migration

import (
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type errorResponse struct {
	Error string `json:"error"`
}

type actionSpecDTO struct {
	ActionID           string   `json:"action_id"`
	VersionID          string   `json:"version_id"`
	ExpectedGeneration int64    `json:"expected_generation"`
	SourceClass        string   `json:"source_class"`
	TargetClass        string   `json:"target_class"`
	SizeBytes          int64    `json:"size_bytes"`
	DependsOn          []string `json:"depends_on"`
}

type submitRequest struct {
	PlanID  string          `json:"plan_id"`
	Actions []actionSpecDTO `json:"actions"`
}

type submitResponse struct {
	Accepted []string `json:"accepted"`
	Existing []string `json:"existing"`
}

type actionDTO struct {
	ActionID           string     `json:"action_id"`
	PlanID             string     `json:"plan_id"`
	VersionID          string     `json:"version_id"`
	ExpectedGeneration int64      `json:"expected_generation"`
	SourceClass        string     `json:"source_class"`
	TargetClass        string     `json:"target_class"`
	SizeBytes          int64      `json:"size_bytes"`
	DependsOn          []string   `json:"depends_on"`
	Status             string     `json:"status"`
	BlockedBy          string     `json:"blocked_by,omitempty"`
	Step               string     `json:"step,omitempty"`
	Reason             string     `json:"reason,omitempty"`
	Attempts           int32      `json:"attempts"`
	BytesCharged       int64      `json:"bytes_charged"`
	CreatedAt          time.Time  `json:"created_at"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

type eventDTO struct {
	At         time.Time `json:"at"`
	FromStatus string    `json:"from_status,omitempty"`
	ToStatus   string    `json:"to_status"`
	FromStep   string    `json:"from_step,omitempty"`
	ToStep     string    `json:"to_step,omitempty"`
	Bytes      int64     `json:"bytes"`
	Reason     string    `json:"reason,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type actionDetailsDTO struct {
	actionDTO
	Events []eventDTO `json:"events"`
}

type listResponse struct {
	Actions       []actionDTO `json:"actions"`
	NextPageToken string      `json:"next_page_token,omitempty"`
}

type cancelRequest struct {
	PlanID    string   `json:"plan_id"`
	ActionIDs []string `json:"action_ids"`
}

type cancelResponse struct {
	Cancelled  []string `json:"cancelled"`
	Dependents []string `json:"dependents_cancelled"`
}

type budgetDTO struct {
	Bytes         int64  `json:"bytes"`
	WindowSeconds int64  `json:"window_seconds"`
	Spent         *int64 `json:"spent,omitempty"`
	Remaining     *int64 `json:"remaining,omitempty"`
}

type settingsDTO struct {
	MinIntervalSeconds int64 `json:"min_interval_seconds"`
	// Paused is read-only here; /pause and /resume change it.
	Paused *bool `json:"paused,omitempty"`
}

type pinDTO struct {
	Bucket    string     `json:"bucket"`
	Key       string     `json:"key"`
	Reason    string     `json:"reason"`
	Author    string     `json:"author"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

func actionFromModel(a *model.Action, blockedBy model.BlockedBy) actionDTO {
	deps := a.DependsOn
	if deps == nil {
		deps = []string{}
	}
	return actionDTO{
		ActionID:           a.ActionID,
		PlanID:             a.PlanID,
		VersionID:          a.VersionID,
		ExpectedGeneration: a.ExpectedGeneration,
		SourceClass:        string(a.SourceClass),
		TargetClass:        string(a.TargetClass),
		SizeBytes:          a.SizeBytes,
		DependsOn:          deps,
		Status:             string(a.Status),
		BlockedBy:          string(blockedBy),
		Step:               string(a.Step),
		Reason:             string(a.Reason),
		Attempts:           a.Attempts,
		BytesCharged:       a.BytesCharged,
		CreatedAt:          a.CreatedAt,
		StartedAt:          a.StartedAt,
		FinishedAt:         a.FinishedAt,
		UpdatedAt:          a.UpdatedAt,
	}
}

func pinFromModel(p model.Pin) pinDTO {
	createdAt := p.CreatedAt
	return pinDTO{Bucket: p.Bucket, Key: p.Key, Reason: p.Reason, Author: p.Author, CreatedAt: &createdAt}
}
