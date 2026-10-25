package service

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type Ingestor interface {
	Ingest(context.Context, model.AccessEvent, model.Delivery) (model.IngestResult, error)
}
