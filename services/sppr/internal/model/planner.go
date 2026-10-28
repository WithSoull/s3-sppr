package model

import (
	"errors"
	"time"
)

// Administrator-facing error codes of the thesis table of exceptional situations.
var (
	ErrPlanStale          = errors.New("PLAN_STALE")
	ErrDependencyRequired = errors.New("DEPENDENCY_REQUIRED")
	ErrHistoryIncomplete  = errors.New("HISTORY_INCOMPLETE")
	ErrModelUnavailable   = errors.New("MODEL_UNAVAILABLE")
	ErrAccessDenied       = errors.New("ACCESS_DENIED")
	ErrPlanNotFound       = errors.New("plan not found")
	ErrCommandNotFound    = errors.New("command not found")
	// ErrCommandConflict: a command_id is already used with other fields.
	ErrCommandConflict = errors.New("command conflicts with an accepted one")
)

const MiB = 1 << 20

// PlannerParams are the coefficients of the thesis placement model. Latency is
// in milliseconds and sizes in MiB, as in the formulas.
type PlannerParams struct {
	Horizon time.Duration
	// c_j: fixed part of one read, ms; v_j: transfer speed, MiB/ms.
	HotFixedMs       float64
	HotMiBPerMs      float64
	ColdFixedMs      float64
	ColdMiBPerMs     float64
	PenaltyMsPerMiB  float64
	MinGainMs        float64
	HotUtilization   float64
	ColdUtilization  float64
	MaxRejectedSaved int
}

type ClassState struct {
	CapacityBytes int64
	// MaxUtilization is Metadata's hard limit; the planner uses the smaller of
	// it and its own utilization setting.
	MaxUtilization float64
	// UsedBytes: placed versions, copies still holding space and targets of
	// accepted actions that have not reserved their copy yet.
	UsedBytes int64
}

// Blocker keeps a version in place regardless of its gain.
type Blocker string

const (
	BlockerNone        Blocker = ""
	BlockerPinned      Blocker = "pinned"
	BlockerMinInterval Blocker = "min_interval"
	BlockerInProgress  Blocker = "in_progress"
)

type PlanItem struct {
	Placement
	// Forecast is nil when no usable forecast exists for a current version.
	Forecast *DemandForecast
	// ReadFraction r_i: mean share of the object read by one read; 1 without reads.
	ReadFraction float64
	Features     *Features
	Blocker      Blocker
}

type PlanInput struct {
	At            time.Time
	Params        PlannerParams
	Hot           ClassState
	Cold          ClassState
	BudgetBytes   int64
	Items         []PlanItem
	MethodVersion string
}

type RecommendationKind string

const (
	KindPromote RecommendationKind = "promote"
	KindEvict   RecommendationKind = "evict"
	// KindRebalance frees HOT that is already over its allowed utilization.
	KindRebalance RecommendationKind = "rebalance"
	// KindEvictNoncurrent moves an overwritten version out of HOT with leftover budget.
	KindEvictNoncurrent RecommendationKind = "evict_noncurrent"
)

type RejectReason string

const (
	RejectNone          RejectReason = ""
	RejectPinned        RejectReason = "pinned"
	RejectMinInterval   RejectReason = "min_interval"
	RejectInProgress    RejectReason = "in_progress"
	RejectBudget        RejectReason = "budget"
	RejectColdCapacity  RejectReason = "cold_capacity"
	RejectHotCapacity   RejectReason = "hot_capacity"
	RejectGainThreshold RejectReason = "gain_below_threshold"
	RejectNoForecast    RejectReason = "no_forecast"
)

type ForecastExplanation struct {
	Hurdle          *HurdleForecast `json:"hurdle,omitempty"`
	Method          string          `json:"method"`
	HorizonSeconds  int64           `json:"horizon_seconds"`
	ExpectedReads   float64         `json:"expected_reads"`
	ReadFraction    float64         `json:"read_fraction"`
	ExpectedMiB     float64         `json:"expected_mib"`
	Quality         string          `json:"quality"`
	LastReadSeconds *float64        `json:"last_read_seconds,omitempty"`
	Reads5m         int64           `json:"reads_5m"`
	Reads1h         int64           `json:"reads_1h"`
	Reads6h         int64           `json:"reads_6h"`
	Reads24h        int64           `json:"reads_24h"`
	EWMA1h          float64         `json:"ewma_1h"`
	// Percentile of ExpectedReads among current versions of the catalogue, 0..100.
	Percentile float64 `json:"percentile"`
}

type DisplacedExplanation struct {
	RecommendationID string  `json:"recommendation_id"`
	VersionID        string  `json:"version_id"`
	SizeBytes        int64   `json:"size_bytes"`
	GainMs           float64 `json:"gain_ms"`
}

type EffectExplanation struct {
	// Set links the actions of one decision: a promotion group and what it displaces.
	Set           string  `json:"set,omitempty"`
	ColdLatencyMs float64 `json:"cold_latency_ms"`
	HotLatencyMs  float64 `json:"hot_latency_ms"`
	// GainMs is Δ_i of this version; the set fields describe the whole decision
	// (a promotion together with what it displaces).
	GainMs       float64                `json:"gain_ms"`
	SetGainMs    float64                `json:"set_gain_ms"`
	SetPenaltyMs float64                `json:"set_penalty_ms"`
	SetNetGainMs float64                `json:"set_net_gain_ms"`
	MinGainMs    float64                `json:"min_gain_ms"`
	Displaced    []DisplacedExplanation `json:"displaced,omitempty"`
}

type ResourceExplanation struct {
	HotUsedBefore  int64 `json:"hot_used_before"`
	HotUsedAfter   int64 `json:"hot_used_after"`
	HotUsable      int64 `json:"hot_usable"`
	ColdUsedBefore int64 `json:"cold_used_before"`
	ColdUsedAfter  int64 `json:"cold_used_after"`
	ColdUsable     int64 `json:"cold_usable"`
	CopyBytes      int64 `json:"copy_bytes"`
	BudgetBefore   int64 `json:"budget_before"`
	BudgetAfter    int64 `json:"budget_after"`
}

// Explanation is built only from values of the planning run. Measured results
// stay in the executor journal and are never written back here.
type Explanation struct {
	Forecast  ForecastExplanation `json:"forecast"`
	Effect    EffectExplanation   `json:"effect"`
	Resources ResourceExplanation `json:"resources"`
	DependsOn []string            `json:"depends_on,omitempty"`
	Reason    RejectReason        `json:"reason,omitempty"`
	Note      string              `json:"note,omitempty"`
}

type Recommendation struct {
	RecommendationID   string
	PlanID             string
	Kind               RecommendationKind
	Selected           bool
	Reason             RejectReason
	VersionID          string
	Bucket             string
	Key                string
	ExpectedGeneration int64
	SourceClass        StorageClass
	TargetClass        StorageClass
	SizeBytes          int64
	DependsOn          []string
	// Order is the position in the plan; rejected candidates follow selected ones.
	Order       int
	Explanation Explanation
}

type PlanSummary struct {
	Actions      int     `json:"actions"`
	CopyBytes    int64   `json:"copy_bytes"`
	NetGainMs    float64 `json:"net_gain_ms"`
	Candidates   int     `json:"candidates"`
	Rejected     int     `json:"rejected"`
	RejectedKept int     `json:"rejected_kept"`
}

type PlanResult struct {
	Recommendations []Recommendation
	Summary         PlanSummary
}

type PlanStatus string

const (
	PlanComputing         PlanStatus = "computing"
	PlanReady             PlanStatus = "ready"
	PlanHistoryIncomplete PlanStatus = "history_incomplete"
	PlanFailed            PlanStatus = "failed"
	// PlanSuperseded: a newer revision exists, so the plan can no longer be confirmed.
	PlanSuperseded PlanStatus = "superseded"
	PlanConfirmed  PlanStatus = "confirmed"
	PlanRejected   PlanStatus = "rejected"
)

type PlanTrigger string

const (
	TriggerTimer  PlanTrigger = "timer"
	TriggerManual PlanTrigger = "manual"
)

type Plan struct {
	PlanID        string
	Revision      int64
	Trigger       PlanTrigger
	Status        PlanStatus
	MethodVersion string
	ModelVersion  string
	Params        PlannerParams
	Summary       PlanSummary
	// Reason explains history_incomplete and failed plans.
	Reason     string
	CreatedAt  time.Time
	FinishedAt *time.Time
}

type PlanFilter struct {
	PageSize int
	// PageToken is the revision to continue below; plans are listed newest first.
	PageToken string
}

type PlanPage struct {
	Plans         []Plan
	NextPageToken string
}

type RecommendationFilter struct {
	PlanID    string
	Selected  *bool
	Kind      RecommendationKind
	PageSize  int
	PageToken string
}

type RecommendationPage struct {
	Recommendations []Recommendation
	NextPageToken   string
}

type CommandType string

const (
	CommandConfirm       CommandType = "confirm"
	CommandReject        CommandType = "reject"
	CommandCancelPending CommandType = "cancel_pending"
	CommandPause         CommandType = "pause"
	CommandResume        CommandType = "resume"
	CommandSettings      CommandType = "settings"
	CommandRecompute     CommandType = "recompute"
	CommandRetrain       CommandType = "retrain"
)

type CommandStatus string

const (
	CommandAccepted CommandStatus = "accepted"
	CommandRejected CommandStatus = "rejected"
)

// CommandRequest is what an administrator sends; CommandID makes a retry return
// the first result instead of acting twice.
type CommandRequest struct {
	CommandID string
	Initiator string
	Type      CommandType
	PlanID    string
	// RecommendationIDs selects part of a plan for confirm; empty means all
	// selected recommendations. For cancel_pending they are action IDs.
	RecommendationIDs []string
	Settings          *PlannerSettings
}

type Command struct {
	CommandRequest
	Status CommandStatus
	// Code is the administrator error code of a rejected command.
	Code    string
	Message string
	// Missing lists recommendations a partial confirm depends on but did not select.
	Missing   []string
	Actions   []string
	CreatedAt time.Time
}

type CommandFilter struct {
	PlanID    string
	PageSize  int
	PageToken string
}

type CommandPage struct {
	Commands      []Command
	NextPageToken string
}

// PlannerSettings are the planner parameters an administrator may change.
type PlannerSettings struct {
	Horizon         time.Duration
	HotFixedMs      float64
	HotMiBPerMs     float64
	ColdFixedMs     float64
	ColdMiBPerMs    float64
	PenaltyMsPerMiB float64
	MinGainMs       float64
	HotUtilization  float64
	ColdUtilization float64
}

// PlannerStatus is the "Состояние" page: what the next plan will be based on.
type ObjectHistory struct {
	VersionRef
	Quality  DataQuality `json:"quality"`
	State    string      `json:"state"`
	Reads24h int64       `json:"reads_24h"`
}
type PlannerStatus struct {
	ModelLifecycle    *ModelLifecycleStatus
	History           Coverage
	HistoryLagSeconds float64
	Objects           []ObjectHistory

	Hot, Cold      ClassState
	HistoryQuality DataQuality
	MethodVersion  string
	ModelVersion   string
	FallbackReason string
	Migration      MigrationSettings
	Budget         BudgetStatus
	LatestPlan     *Plan
	PendingActions int
	RunningActions int
}
