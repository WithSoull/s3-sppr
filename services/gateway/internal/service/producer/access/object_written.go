package access

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *accessProducerService) ProduceObjectWritten(ctx context.Context, event model.ObjectWrittenEvent) {
	err := s.objectWrittenStream.publish(ctx, s.objectWrittenProducer, s.gatewayID,
		func(position *events_v1.StreamPosition, occurredAt *timestamppb.Timestamp) proto.Message {
			return &events_v1.ObjectWritten{
				Position:   position,
				OccurredAt: occurredAt,
				Bucket:     event.Bucket,
				ObjectKey:  event.ObjectKey,
				VersionId:  event.VersionID,
				Bytes:      event.Bytes,
				Origin:     origin(event.Origin),
			}
		})
	if err != nil {
		s.drop(ctx, "object_written", err)
	}
}
