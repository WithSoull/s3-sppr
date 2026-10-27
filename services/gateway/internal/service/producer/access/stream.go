package access

import (
	"context"
	"sync"
	"time"

	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// stream numbers the events of one topic. Kafka does not order two topics
// against each other, so a counter shared between topics would show false
// gaps in SPPR.
type stream struct {
	id       string
	mu       sync.Mutex
	sequence int64
}

func newStream() *stream {
	return &stream{id: uuid.NewString()}
}

// publish numbers, marshals and sends under one lock; otherwise event N+1 could
// reach sarama before N. Send does not wait for Kafka, so the lock is short.
// A failed Send keeps its number spent, and SPPR records the loss as a gap.
func (s *stream) publish(
	ctx context.Context,
	producer kafka.Producer,
	gatewayID string,
	message func(*events_v1.StreamPosition, *timestamppb.Timestamp) proto.Message,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	position, occurredAt := s.next(gatewayID)
	payload, err := proto.Marshal(message(position, occurredAt))
	if err != nil {
		return err
	}
	return producer.Send(ctx, []byte(gatewayID+"/"+s.id), payload, nil)
}

// next must be called with mu held: the time is taken together with the number.
func (s *stream) next(gatewayID string) (*events_v1.StreamPosition, *timestamppb.Timestamp) {
	s.sequence++
	position := &events_v1.StreamPosition{GatewayId: gatewayID, StreamId: s.id, Sequence: s.sequence}
	return position, timestamppb.New(time.Now().UTC())
}
