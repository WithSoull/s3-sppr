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
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
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
		return model.IngestResult{}, fmt.Errorf("%w: %w", model.ErrRejected, err)
	}
	if delivery.Topic == "" || delivery.Partition < 0 || delivery.Offset < 0 {
		return model.IngestResult{}, fmt.Errorf("%w: invalid Kafka delivery coordinates", model.ErrRejected)
	}
	// Match PostgreSQL timestamp precision before comparing progress.
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
	if result.Resumed {
		logger.With().Info(ctx, "lost stream resumed", zap.String("gateway_id", event.GatewayID), zap.String("stream_id", event.StreamID), zap.Int64("sequence", event.Sequence))
	}
	return result, nil
}

func (s *ingestor) Heartbeat(ctx context.Context, h model.StreamHeartbeat, d model.Delivery) (model.IngestResult, error) {
	if err := validator.Heartbeat(h); err != nil {
		return model.IngestResult{}, fmt.Errorf("%w: %w", model.ErrRejected, err)
	}
	if d.Topic == "" || d.Partition < 0 || d.Offset < 0 {
		return model.IngestResult{}, fmt.Errorf("%w: invalid Kafka delivery coordinates", model.ErrRejected)
	}
	h.At = h.At.UTC().Truncate(time.Microsecond)
	var result model.IngestResult
	err := s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		var err error
		result, err = s.repo.StoreHeartbeat(ctx, h, d)
		return err
	})
	if err != nil {
		return model.IngestResult{}, err
	}
	if result.Resumed {
		logger.With().Info(ctx, "lost stream resumed", zap.String("gateway_id", h.GatewayID), zap.String("stream_id", h.StreamID), zap.Int64("sequence", h.Sequence))
	}
	return result, nil
}
func (s *ingestor) CheckLostStreams(ctx context.Context, now time.Time, after time.Duration) error {
	return s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		return s.repo.CloseLostStreams(ctx, now.UTC().Truncate(time.Microsecond), after)
	})
}

// RunLostCheck uses an injected clock; database errors are retried on the next tick.
func RunLostCheck(ctx context.Context, s service.Ingestor, after time.Duration, now func() time.Time) {
	interval := after / 6
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.CheckLostStreams(ctx, now(), after); err != nil && ctx.Err() == nil {
			logger.Error(ctx, "lost stream check failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
