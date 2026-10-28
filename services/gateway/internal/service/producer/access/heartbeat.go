package access

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	producer "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/producer"
	events "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *stream) heartbeat(ctx context.Context, p kafka.Producer, gateway string, closing, confirm bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, err := proto.Marshal(&events.StreamHeartbeat{Position: &events.StreamPosition{GatewayId: gateway, StreamId: s.id, Sequence: s.sequence}, At: timestamppb.Now(), Closing: closing})
	if err != nil {
		return err
	}
	ctx = producer.WithHeaders(ctx, sarama.RecordHeader{Key: []byte("event-type"), Value: []byte("heartbeat")})
	var ch chan error
	if confirm {
		ch = make(chan error, 1)
		ctx = producer.WithReceipt(ctx, ch)
	}
	for {
		err = p.Send(ctx, []byte(gateway+"/"+s.id), payload, nil)
		if err == nil || !confirm {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	if err != nil {
		return err
	}
	if confirm {
		select {
		case err = <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err == nil && closing {
		s.closed = true
	}
	return err
}

// Start is called once by the provider; constructor-only unit tests need no goroutine.
func (s *accessProducerService) Start(interval time.Duration) {
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				if err := s.Heartbeat(context.Background(), false, false); err != nil {
					s.drop(context.Background(), "heartbeat", err)
				}
			}
		}
	}()
}
func (s *accessProducerService) Heartbeat(ctx context.Context, closing, confirm bool) error {
	var result error
	for _, v := range []struct {
		s *stream
		p kafka.Producer
	}{{s.objectReadStream, s.objectReadProducer}, {s.objectWrittenStream, s.objectWrittenProducer}} {
		if err := v.s.heartbeat(ctx, v.p, s.gatewayID, closing, confirm); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (s *accessProducerService) stopPeriodic(ctx context.Context) error {
	if s.stop != nil {
		s.stopOnce.Do(func() { close(s.stop) })
		select {
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Abort stops publication without claiming a complete graceful close. Used when
// HTTP did not drain; a late operation must not send after sarama is closed.
func (s *accessProducerService) Abort(ctx context.Context) error {
	err := s.stopPeriodic(ctx)
	for _, stream := range []*stream{s.objectReadStream, s.objectWrittenStream} {
		stream.mu.Lock()
		stream.closed = true
		stream.mu.Unlock()
	}
	return err
}

// Stop runs after HTTP drains, before the shared AsyncProducer closes. Confirmed
// ordinary barriers drain both topic buffers before the closing barriers.
func (s *accessProducerService) Stop(ctx context.Context) error {
	if err := s.stopPeriodic(ctx); err != nil {
		return err
	}
	if err := s.Heartbeat(ctx, false, true); err != nil {
		return err
	}
	return s.Heartbeat(ctx, true, true)
}
