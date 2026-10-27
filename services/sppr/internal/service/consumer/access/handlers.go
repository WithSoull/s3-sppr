package access

import (
	"context"
	"errors"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

const (
	outcomeStored    = "stored"
	outcomeDuplicate = "duplicate"
	outcomeRejected  = "rejected"
	outcomeError     = "error"
)

func (s *accessConsumerService) ObjectReadHandler(ctx context.Context, msg kafka.Message) error {
	return s.handle(ctx, msg, s.objectReadDecoder.Decode)
}

func (s *accessConsumerService) ObjectWrittenHandler(ctx context.Context, msg kafka.Message) error {
	return s.handle(ctx, msg, s.objectWrittenDecoder.Decode)
}

// A rejected event returns nil so its offset is committed: retrying cannot fix
// it, and its sequence stays visible as a gap. Any other error goes back to
// Retry, so the partition waits instead of committing past an unstored event.
func (s *accessConsumerService) handle(ctx context.Context, msg kafka.Message, decode func([]byte) (model.AccessEvent, error)) error {
	start := time.Now()
	event, err := decode(msg.Value)
	if err != nil {
		s.reject(ctx, msg, start, err)
		return nil
	}

	result, err := s.ingestor.Ingest(ctx, event, model.Delivery{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
	if errors.Is(err, model.ErrRejected) {
		s.reject(ctx, msg, start, err)
		return nil
	}
	if err != nil {
		logger.Error(ctx, "Failed to ingest access event; retrying", messageFields(msg, err)...)
		s.observer.Observe(ctx, outcomeError, 0, time.Since(start))
		return err
	}

	outcome := outcomeStored
	if result.Duplicate {
		outcome = outcomeDuplicate
	}
	s.observer.Observe(ctx, outcome, result.Missing, time.Since(start))
	return nil
}

func (s *accessConsumerService) reject(ctx context.Context, msg kafka.Message, start time.Time, err error) {
	logger.Error(ctx, "Access event rejected", messageFields(msg, err)...)
	s.observer.Observe(ctx, outcomeRejected, 0, time.Since(start))
}

func messageFields(msg kafka.Message, err error) []zap.Field {
	return []zap.Field{
		zap.String("topic", msg.Topic),
		zap.Int32("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.Error(err),
	}
}
