package ingestion

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/stretchr/testify/require"
)

func TestRetryKeepsFailedMessageUntilSuccess(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP", "localhost:9092")
	t.Setenv("KAFKA_RETRY_INTERVAL", "1ms")
	cfg, err := env.NewKafkaConfig()
	require.NoError(t, err)
	w := &Worker{cfg: cfg, log: &logger.NoopLogger{}}
	w.sessions.Store(1)
	var attempts int
	msg := kafka.Message{Offset: 7, Partition: 2}
	err = w.retry(func(ctx context.Context, received kafka.Message) error {
		require.Equal(t, msg, received)
		_, deadline := ctx.Deadline()
		require.True(t, deadline)
		attempts++
		if attempts < 3 {
			return errors.New("database offline")
		}
		require.False(t, w.Ready())
		return nil
	})(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	require.True(t, w.Ready())
}

func TestRetryStopsOnCancellationWithoutSuccess(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP", "localhost:9092")
	t.Setenv("KAFKA_RETRY_INTERVAL", "1h")
	cfg, err := env.NewKafkaConfig()
	require.NoError(t, err)
	w := &Worker{cfg: cfg, log: &logger.NoopLogger{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int64
	processed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- w.retry(func(context.Context, kafka.Message) error {
			attempts.Add(1)
			close(processed)
			return errors.New("poison event")
		})(ctx, kafka.Message{})
	}()
	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("handler did not run")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("retry ignored cancellation")
	}
	require.Equal(t, int64(1), attempts.Load())
}
