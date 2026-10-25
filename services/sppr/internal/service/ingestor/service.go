package ingestor

import (
	"context"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/validator"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

type ingestor struct {
	repo repository.EventRepository
	tx   db.TxManager
}

func NewService(repo repository.EventRepository, tx db.TxManager) service.Ingestor {
	return &ingestor{repo: repo, tx: tx}
}

func (s *ingestor) Ingest(ctx context.Context, event model.AccessEvent, delivery model.Delivery) (model.IngestResult, error) {
	if err := validator.AccessEvent(event); err != nil {
		return model.IngestResult{}, fmt.Errorf("validate access event: %w", err)
	}
	if delivery.Topic == "" || delivery.Partition < 0 || delivery.Offset < 0 {
		return model.IngestResult{}, fmt.Errorf("invalid Kafka delivery coordinates")
	}
	// Match PostgreSQL timestamp precision before hashing and comparing progress.
	event.OccurredAt = event.OccurredAt.UTC().Truncate(time.Microsecond)
	var result model.IngestResult
	err := s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		var err error
		result, err = s.repo.Store(ctx, event, delivery)
		return err
	})
	if err != nil {
		return model.IngestResult{}, fmt.Errorf("persist access event: %w", err)
	}
	return result, nil
}
