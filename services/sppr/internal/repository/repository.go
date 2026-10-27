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
	// Claim takes the oldest pending action whose dependencies succeeded and whose
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
