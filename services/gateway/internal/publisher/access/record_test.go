package access

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/config"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/config/env"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	event "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/events/access"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func testConfig(t *testing.T, capacity int) config.AccessConfig {
	t.Helper()
	t.Setenv("ACCESS_EVENTS_ENABLED", "true")
	t.Setenv("GATEWAY_ID", "gw")
	t.Setenv("KAFKA_BOOTSTRAP", "localhost:9092")
	t.Setenv("ACCESS_EVENTS_RETRY_INTERVAL", "1ms")
	cfg, err := env.NewAccessConfig()
	require.NoError(t, err)
	// Capacity is independent of the env parsing tested in config/env.
	cfg.Capacity = capacity
	return cfg
}
func readEvent() event.Event {
	return event.Event{Bucket: "bucket", ObjectKey: "key", VersionID: "v1", Operation: "read", Origin: "client", Result: "success", Bytes: 4}
}
func closePublisher(t *testing.T, p *Publisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, p.Close(ctx))
}

func TestRecordConcurrentOrderAndDetachedTrace(t *testing.T) {
	mc := minimock.NewController(t)
	sender := mocks.NewProducerMock(mc)
	var received []event.Event
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	sender.SendMock.Set(func(ctx context.Context, key, value []byte, _ kafka.PrettyDecoder) error {
		assert.Equal(t, "gw/run", string(key))
		assert.NoError(t, ctx.Err())
		assert.Equal(t, sc, trace.SpanContextFromContext(ctx))
		var e event.Event
		if err := json.Unmarshal(value, &e); err != nil {
			return err
		}
		received = append(received, e)
		return nil
	})
	p, err := NewPublisher(testConfig(t, 256), sender, &logger.NoopLogger{}, "run")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(trace.ContextWithSpanContext(context.Background(), sc))
	cancel()
	var wg sync.WaitGroup
	for range 128 {
		wg.Add(1)
		go func() { defer wg.Done(); p.Record(ctx, readEvent()) }()
	}
	wg.Wait()
	closePublisher(t, p)
	require.Len(t, received, 128)
	for i, e := range received {
		require.NoError(t, event.Validate(e))
		require.Equal(t, int64(i+1), e.Sequence)
		require.Equal(t, time.UTC, e.OccurredAt.Location())
		require.Zero(t, e.OccurredAt.Nanosecond()%1000)
		if i > 0 {
			require.False(t, e.OccurredAt.Before(received[i-1].OccurredAt))
		}
	}
	require.Zero(t, p.pending.Load())
}

func TestRetryPreservesPayloadBeforeNextEvent(t *testing.T) {
	mc := minimock.NewController(t)
	sender := mocks.NewProducerMock(mc)
	var payloads [][]byte
	sender.SendMock.Set(func(_ context.Context, _, value []byte, _ kafka.PrettyDecoder) error {
		payloads = append(payloads, append([]byte(nil), value...))
		if len(payloads) == 1 {
			return errors.New("Kafka unavailable")
		}
		return nil
	})
	p, err := NewPublisher(testConfig(t, 4), sender, &logger.NoopLogger{}, "run")
	require.NoError(t, err)
	p.Record(context.Background(), readEvent())
	p.Record(context.Background(), readEvent())
	closePublisher(t, p)
	require.Len(t, payloads, 3)
	require.Equal(t, payloads[0], payloads[1])
	var next event.Event
	require.NoError(t, json.Unmarshal(payloads[2], &next))
	require.Equal(t, int64(2), next.Sequence)
}

func TestQueueOverflowLeavesSequenceGap(t *testing.T) {
	mc := minimock.NewController(t)
	sender := mocks.NewProducerMock(mc)
	entered, release, secondSent := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var sequences []int64
	sender.SendMock.Set(func(ctx context.Context, _, value []byte, _ kafka.PrettyDecoder) error {
		var e event.Event
		if err := json.Unmarshal(value, &e); err != nil {
			return err
		}
		sequences = append(sequences, e.Sequence)
		switch e.Sequence {
		case 1:
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		case 2:
			close(secondSent)
		}
		return nil
	})
	p, err := NewPublisher(testConfig(t, 1), sender, &logger.NoopLogger{}, "run")
	require.NoError(t, err)
	p.Record(context.Background(), readEvent())
	<-entered
	p.Record(context.Background(), readEvent())
	p.Record(context.Background(), readEvent())
	require.Equal(t, int64(1), p.dropped.Load())
	close(release)
	<-secondSent
	p.Record(context.Background(), readEvent())
	closePublisher(t, p)
	require.Equal(t, []int64{1, 2, 4}, sequences)
}

func TestCloseCancelsBlockedSendAndConcurrentRecord(t *testing.T) {
	mc := minimock.NewController(t)
	sender := mocks.NewProducerMock(mc)
	entered := make(chan struct{})
	sender.SendMock.Set(func(ctx context.Context, _, _ []byte, _ kafka.PrettyDecoder) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	p, err := NewPublisher(testConfig(t, 1), sender, &logger.NoopLogger{}, "run")
	require.NoError(t, err)
	p.Record(context.Background(), readEvent())
	<-entered
	p.Record(context.Background(), readEvent())
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() { defer wg.Done(); p.Record(context.Background(), readEvent()) }()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, p.Close(ctx), context.Canceled)
	wg.Wait()
	require.Zero(t, p.pending.Load())
	select {
	case <-p.done:
	default:
		t.Fatal("sender still running after Close")
	}
	before := p.dropped.Load()
	p.Record(context.Background(), readEvent())
	require.Equal(t, before+1, p.dropped.Load())
}
