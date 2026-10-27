package repository

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type EventRepository interface {
	// Store requires a transaction; identical redelivery is a no-op.
	Store(context.Context, model.AccessEvent, model.Delivery) (model.IngestResult, error)
}

type ActionRepository interface {
	// Get returns model.ErrActionNotFound for an unknown ID.
	Get(ctx context.Context, actionID string) (*model.Action, error)
	// GetMany omits unknown IDs from the result.
	GetMany(ctx context.Context, actionIDs []string) (map[string]*model.Action, error)
	// List returns up to filter.PageSize actions ordered by action_id after PageToken.
	List(ctx context.Context, filter model.ActionFilter) ([]*model.Action, error)
	ListEvents(ctx context.Context, actionID string) ([]model.ActionEvent, error)
	ListRunning(ctx context.Context) ([]*model.Action, error)
	// Insert stores pending actions with their dependency edges; requires a transaction.
	Insert(ctx context.Context, specs []model.ActionSpec) error
	// ListAwaitingSourceCleanup returns succeeded actions whose source blob is not
	// known to be deleted yet while a pending action depends on them.
	ListAwaitingSourceCleanup(ctx context.Context, limit int) ([]string, error)
	MarkSourceCleaned(ctx context.Context, actionID string) error
	// ListActive returns pending and running actions for planning.
	ListActive(ctx context.Context) ([]ActiveAction, error)
	// Claim takes the oldest pending action whose dependencies succeeded with
	// their source blob deleted (so the space they free is really free) and whose
	// size fits maxBytes, skipping rows locked by other claims, charges its size
	// and marks it running/reserving. It returns nil when nothing is runnable and
	// requires a transaction.
	Claim(ctx context.Context, maxBytes int64) (*model.Action, error)
	// Transition applies t if the action is still in t.FromStatus/t.FromStep and
	// returns pending dependents it cancelled; requires a transaction.
	Transition(ctx context.Context, t model.Transition) ([]string, error)
	// CancelPending cancels pending actions with reason user; requires a transaction.
	CancelPending(ctx context.Context, req model.CancelRequest) ([]string, error)
	// CancelDependents cancels pending actions that transitively depend on
	// actionIDs with reason dependency_failed; requires a transaction.
	CancelDependents(ctx context.Context, actionIDs []string) ([]string, error)
}

type SettingsRepository interface {
	// EnsureDefaults stores defaults only if no settings exist yet.
	EnsureDefaults(ctx context.Context, defaults model.MigrationSettings) error
	Get(ctx context.Context) (model.MigrationSettings, error)
	// GetForUpdate locks the settings row, so claims and changes that check the
	// budget run one at a time; requires a transaction.
	GetForUpdate(ctx context.Context) (model.MigrationSettings, error)
	Update(ctx context.Context, settings model.MigrationSettings) error
	// Spent sums budget charges made within the last window.
	Spent(ctx context.Context, window time.Duration) (int64, error)
}

type PinRepository interface {
	List(ctx context.Context) ([]model.Pin, error)
	// Create returns model.ErrPinExists for a pinned bucket/key.
	Create(ctx context.Context, pin model.Pin) (model.Pin, error)
	// Delete returns model.ErrPinNotFound when nothing was pinned.
	Delete(ctx context.Context, bucket, key string) error
	IsPinned(ctx context.Context, bucket, key string) (bool, error)
}

type PlanRepository interface {
	// EnsureSettings stores defaults only if no planner settings exist yet.
	EnsureSettings(ctx context.Context, defaults model.PlannerSettings) error
	GetSettings(ctx context.Context) (model.PlannerSettings, error)
	UpdateSettings(ctx context.Context, settings model.PlannerSettings) error

	// CreatePlan stores a computing plan and returns it with its revision.
	CreatePlan(ctx context.Context, plan model.Plan) (model.Plan, error)
	// FinishPlan stores the result, marks older ready plans superseded and
	// requires a transaction.
	FinishPlan(ctx context.Context, plan model.Plan, recs []model.Recommendation) error
	// FailInterrupted marks plans left computing by a previous process as failed.
	FailInterrupted(ctx context.Context) error
	// Get returns model.ErrPlanNotFound for an unknown ID; ForUpdate locks the
	// row and requires a transaction.
	GetPlan(ctx context.Context, planID string, forUpdate bool) (model.Plan, error)
	// LatestPlan returns the finished plan with the highest revision, or nil.
	LatestPlan(ctx context.Context) (*model.Plan, error)
	ListPlans(ctx context.Context, filter model.PlanFilter) ([]model.Plan, error)
	SetPlanStatus(ctx context.Context, planID string, status model.PlanStatus) error
	// PrunePlans keeps the newest keep plans that were never confirmed.
	PrunePlans(ctx context.Context, keep int) error

	ListRecommendations(ctx context.Context, filter model.RecommendationFilter) ([]model.Recommendation, error)
	// SelectedRecommendations returns every recommendation selected into the plan.
	SelectedRecommendations(ctx context.Context, planID string) ([]model.Recommendation, error)

	// GetCommand returns model.ErrCommandNotFound for an unknown ID.
	GetCommand(ctx context.Context, commandID string) (model.Command, error)
	InsertCommand(ctx context.Context, cmd model.Command) error
	ListCommands(ctx context.Context, filter model.CommandFilter) ([]model.Command, error)
	// AttachPlan records the plan a recompute command produced.
	AttachPlan(ctx context.Context, commandIDs []string, planID string) error
}

type ActiveAction struct {
	VersionID   string
	TargetClass model.StorageClass
	SizeBytes   int64
	Status      model.ActionStatus
	Step        model.ActionStep
}

// HistoryRepository reads what the Ingestor stored for planning.
type HistoryRepository interface {
	// ReadsSince returns per-minute client reads of every version since from.
	ReadsSince(ctx context.Context, from time.Time) ([]model.MinuteReads, error)
	// Coverage reports what the Ingestor knows about the completeness of history.
	Coverage(ctx context.Context, since time.Time) (model.Coverage, error)
}
