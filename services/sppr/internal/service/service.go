package service

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type Ingestor interface {
	Ingest(context.Context, model.AccessEvent, model.Delivery) (model.IngestResult, error)
}

type AccessConsumerService interface {
	RunConsumer(ctx context.Context) error
}

// MigrationService is the administrator API of the migration executor. The HTTP
// handler is a thin adapter, so a Planner can call the same methods directly.
type MigrationService interface {
	// SubmitActions accepts a plan's actions. Resubmitting an action with the
	// same fields is a no-op; with other fields it fails with model.ErrActionConflict.
	SubmitActions(ctx context.Context, planID string, actions []model.ActionSpec) (model.SubmitResult, error)
	ListActions(ctx context.Context, filter model.ActionFilter) (model.ActionPage, error)
	GetAction(ctx context.Context, actionID string) (model.ActionDetails, error)
	// CancelActions cancels only pending actions and the actions depending on them.
	CancelActions(ctx context.Context, req model.CancelRequest) (model.CancelResult, error)
	SetPaused(ctx context.Context, paused bool) error
	GetBudget(ctx context.Context) (model.BudgetStatus, error)
	SetBudget(ctx context.Context, bytes int64, window time.Duration) error
	GetSettings(ctx context.Context) (model.MigrationSettings, error)
	SetMinInterval(ctx context.Context, interval time.Duration) error
	ListPins(ctx context.Context) ([]model.Pin, error)
	AddPin(ctx context.Context, pin model.Pin) (model.Pin, error)
	DeletePin(ctx context.Context, bucket, key string) error
}

type MigrationExecutor interface {
	// Run resumes running actions and claims pending ones on a worker pool until
	// ctx is cancelled. An interrupted action stays running and is resumed later.
	Run(ctx context.Context) error
	// Step resumes or claims one action and processes it to completion or until
	// it has to wait; it reports whether there was an action to process.
	Step(ctx context.Context) (bool, error)
}

type MigrationCleanup interface {
	Run(ctx context.Context) error
	Step(ctx context.Context) error
}

type MigrationObserver interface {
	Actions(ctx context.Context, outcome, reason string, count int64)
	Copied(ctx context.Context, targetClass string, bytes int64)
	Duration(ctx context.Context, elapsed time.Duration)
	Cleanup(ctx context.Context, outcome string)
}

type FeatureBuilder interface {
	Build(context.Context, model.FeatureRequest) ([]model.FeatureSnapshot, error)
}

// Predictor turns a feature snapshot into a demand forecast.
type Predictor interface {
	MethodVersion() string
	// ModelVersion is empty for methods without a model artifact (EWMA).
	ModelVersion() string
	Predict(snapshot model.FeatureSnapshot, horizon time.Duration) (model.DemandForecast, error)
}

// PlannerService computes plan revisions and applies administrator commands.
// The HTTP handler and the HTML pages are thin adapters over it.
type PlannerService interface {
	// Run computes a plan at start, on the timer and on recompute commands
	// until ctx is cancelled.
	Run(ctx context.Context) error
	Plan(ctx context.Context, trigger model.PlanTrigger) (model.Plan, error)
	// Execute applies a command once per CommandID. A command the rules refuse
	// (stale plan, missing dependency) is stored and returned with status
	// rejected and its code, not as an error.
	Execute(ctx context.Context, req model.CommandRequest) (model.Command, error)
	GetCommand(ctx context.Context, commandID string) (model.Command, error)
	ListCommands(ctx context.Context, filter model.CommandFilter) (model.CommandPage, error)
	GetPlan(ctx context.Context, planID string) (model.Plan, error)
	LatestPlan(ctx context.Context) (*model.Plan, error)
	ListPlans(ctx context.Context, filter model.PlanFilter) (model.PlanPage, error)
	ListRecommendations(ctx context.Context, filter model.RecommendationFilter) (model.RecommendationPage, error)
	GetSettings(ctx context.Context) (model.PlannerSettings, error)
	Status(ctx context.Context) (model.PlannerStatus, error)
}
