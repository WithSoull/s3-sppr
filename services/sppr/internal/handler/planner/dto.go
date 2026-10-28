package planner

import (
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type errorResponse struct {
	Code  string `json:"code,omitempty"`
	Error string `json:"error"`
}

type paramsDTO struct {
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

func paramsFromSettings(s model.PlannerSettings) paramsDTO {
	return paramsDTO{
		HorizonSeconds: int64(s.Horizon / time.Second),
		HotFixedMs:     s.HotFixedMs, HotMiBPerMs: s.HotMiBPerMs, ColdFixedMs: s.ColdFixedMs, ColdMiBPerMs: s.ColdMiBPerMs,
		PenaltyMsPerMiB: s.PenaltyMsPerMiB, MinGainMs: s.MinGainMs,
		HotUtilization: s.HotUtilization, ColdUtilization: s.ColdUtilization,
	}
}

func (p paramsDTO) toModel() model.PlannerSettings {
	return model.PlannerSettings{
		Horizon:    time.Duration(p.HorizonSeconds) * time.Second,
		HotFixedMs: p.HotFixedMs, HotMiBPerMs: p.HotMiBPerMs, ColdFixedMs: p.ColdFixedMs, ColdMiBPerMs: p.ColdMiBPerMs,
		PenaltyMsPerMiB: p.PenaltyMsPerMiB, MinGainMs: p.MinGainMs,
		HotUtilization: p.HotUtilization, ColdUtilization: p.ColdUtilization,
	}
}

type planDTO struct {
	PlanID        string            `json:"plan_id"`
	Revision      int64             `json:"revision"`
	Trigger       string            `json:"trigger"`
	Status        string            `json:"status"`
	MethodVersion string            `json:"method_version"`
	ModelVersion  string            `json:"model_version"`
	Params        paramsDTO         `json:"params"`
	Summary       model.PlanSummary `json:"summary"`
	Reason        string            `json:"reason,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	FinishedAt    *time.Time        `json:"finished_at,omitempty"`
}

func planFromModel(p model.Plan) planDTO {
	pr := p.Params
	return planDTO{
		PlanID: p.PlanID, Revision: p.Revision, Trigger: string(p.Trigger), Status: string(p.Status),
		MethodVersion: p.MethodVersion, ModelVersion: p.ModelVersion, Summary: p.Summary, Reason: p.Reason, CreatedAt: p.CreatedAt, FinishedAt: p.FinishedAt,
		Params: paramsFromSettings(model.PlannerSettings{
			Horizon: pr.Horizon, HotFixedMs: pr.HotFixedMs, HotMiBPerMs: pr.HotMiBPerMs, ColdFixedMs: pr.ColdFixedMs,
			ColdMiBPerMs: pr.ColdMiBPerMs, PenaltyMsPerMiB: pr.PenaltyMsPerMiB, MinGainMs: pr.MinGainMs,
			HotUtilization: pr.HotUtilization, ColdUtilization: pr.ColdUtilization,
		}),
	}
}

type recommendationDTO struct {
	RecommendationID   string            `json:"recommendation_id"`
	Order              int               `json:"order"`
	Kind               string            `json:"kind"`
	Selected           bool              `json:"selected"`
	Reason             string            `json:"reason,omitempty"`
	VersionID          string            `json:"version_id"`
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	ExpectedGeneration int64             `json:"expected_generation"`
	SourceClass        string            `json:"source_class"`
	TargetClass        string            `json:"target_class"`
	SizeBytes          int64             `json:"size_bytes"`
	DependsOn          []string          `json:"depends_on"`
	Explanation        model.Explanation `json:"explanation"`
}

func recommendationFromModel(r model.Recommendation) recommendationDTO {
	deps := r.DependsOn
	if deps == nil {
		deps = []string{}
	}
	return recommendationDTO{
		RecommendationID: r.RecommendationID, Order: r.Order, Kind: string(r.Kind), Selected: r.Selected,
		Reason: string(r.Reason), VersionID: r.VersionID, Bucket: r.Bucket, Key: r.Key,
		ExpectedGeneration: r.ExpectedGeneration, SourceClass: string(r.SourceClass), TargetClass: string(r.TargetClass),
		SizeBytes: r.SizeBytes, DependsOn: deps, Explanation: r.Explanation,
	}
}

type commandDTO struct {
	CommandID         string     `json:"command_id"`
	Type              string     `json:"type"`
	Initiator         string     `json:"initiator"`
	PlanID            string     `json:"plan_id,omitempty"`
	RecommendationIDs []string   `json:"recommendation_ids,omitempty"`
	Settings          *paramsDTO `json:"settings,omitempty"`
	Status            string     `json:"status"`
	Code              string     `json:"code,omitempty"`
	Message           string     `json:"message,omitempty"`
	Missing           []string   `json:"missing,omitempty"`
	Actions           []string   `json:"actions,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
}

func commandFromModel(c model.Command) commandDTO {
	dto := commandDTO{
		CommandID: c.CommandID, Type: string(c.Type), Initiator: c.Initiator, PlanID: c.PlanID,
		RecommendationIDs: c.RecommendationIDs, Status: string(c.Status), Code: c.Code, Message: c.Message,
		Missing: c.Missing, Actions: c.Actions, CreatedAt: c.CreatedAt,
	}
	if c.Settings != nil {
		p := paramsFromSettings(*c.Settings)
		dto.Settings = &p
	}
	return dto
}

// commandRequest is the body of every command; command_id is the client's
// idempotency key, so a retry after a lost response returns the first result.
type commandRequest struct {
	CommandID         string   `json:"command_id"`
	RecommendationIDs []string `json:"recommendation_ids,omitempty"`
	PlanID            string   `json:"plan_id,omitempty"`
	ActionIDs         []string `json:"action_ids,omitempty"`
}

type settingsRequest struct {
	CommandID string `json:"command_id"`
	paramsDTO
}

type classDTO struct {
	CapacityBytes  int64   `json:"capacity_bytes"`
	MaxUtilization float64 `json:"max_utilization"`
	UsedBytes      int64   `json:"used_bytes"`
}

type statusDTO struct {
	History           model.Coverage        `json:"history"`
	HistoryLagSeconds float64               `json:"history_lag_seconds"`
	Objects           []model.ObjectHistory `json:"objects"`
	Hot               classDTO              `json:"hot"`
	Cold              classDTO              `json:"cold"`
	HistoryQuality    model.DataQuality     `json:"history_quality"`
	MethodVersion     string                `json:"method_version"`
	ModelVersion      string                `json:"model_version"`
	FallbackReason    string                `json:"fallback_reason"`
	Paused            bool                  `json:"paused"`
	MinIntervalSec    int64                 `json:"min_interval_seconds"`
	BudgetBytes       int64                 `json:"budget_bytes"`
	BudgetWindowS     int64                 `json:"budget_window_seconds"`
	BudgetSpent       int64                 `json:"budget_spent"`
	BudgetLeft        int64                 `json:"budget_remaining"`
	LatestPlan        *planDTO              `json:"latest_plan,omitempty"`
	PendingActions    int                   `json:"pending_actions"`
	RunningActions    int                   `json:"running_actions"`
}
