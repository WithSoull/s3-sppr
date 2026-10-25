package access

import (
	"context"
	"errors"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	kafkaProducer "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/producer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type accessProducerService struct {
	gatewayID string

	objectReadStream   *stream
	objectReadProducer kafka.Producer

	objectWrittenStream   *stream
	objectWrittenProducer kafka.Producer

	dropped metric.Int64Counter
}

func NewService(
	gatewayID string,
	objectReadProducer kafka.Producer,
	objectWrittenProducer kafka.Producer,
	dropped metric.Int64Counter,
) service.AccessProducerService {
	return &accessProducerService{
		gatewayID: gatewayID,

		objectReadStream:   newStream(),
		objectReadProducer: objectReadProducer,

		objectWrittenStream:   newStream(),
		objectWrittenProducer: objectWrittenProducer,

		dropped: dropped,
	}
}

// drop records an event that SPPR will see as a gap.
func (s *accessProducerService) drop(ctx context.Context, event string, err error) {
	reason := "error"
	if errors.Is(err, kafkaProducer.ErrBufferFull) {
		reason = "buffer_full"
	}
	logger.Error(ctx, "Access event dropped", zap.String("event", event), zap.String("reason", reason), zap.Error(err))
	s.dropped.Add(ctx, 1, metric.WithAttributes(attribute.String("event", event), attribute.String("reason", reason)))
}

func origin(o model.Origin) events_v1.Origin {
	switch o {
	case model.OriginClient:
		return events_v1.Origin_ORIGIN_CLIENT
	case model.OriginMigration:
		return events_v1.Origin_ORIGIN_MIGRATION
	default:
		return events_v1.Origin_ORIGIN_UNSPECIFIED
	}
}
