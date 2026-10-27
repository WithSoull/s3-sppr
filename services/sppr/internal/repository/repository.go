package repository

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type EventRepository interface {
	// Store requires a transaction; identical redelivery is a no-op.
	Store(context.Context, model.AccessEvent, model.Delivery) (model.IngestResult, error)
}
