package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	kafkaMiddleware "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/middleware/kafka"
	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	failure := errors.New("database unavailable")
	tests := []struct {
		name         string
		failures     int
		cancelAfter  int
		preCancelled bool
		wantCalls    int
		wantErr      error
	}{
		{name: "first attempt succeeds", wantCalls: 1},
		{name: "retries until success", failures: 2, wantCalls: 3},
		{name: "stops on cancellation", failures: 100, cancelAfter: 3, wantCalls: 3, wantErr: context.Canceled},
		{name: "skips message after cancellation", preCancelled: true, wantCalls: 0, wantErr: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.preCancelled {
				cancel()
			}
			calls := 0
			handler := kafkaMiddleware.Retry(time.Millisecond)(func(context.Context, kafka.Message) error {
				calls++
				if calls == tt.cancelAfter {
					cancel()
				}
				if calls <= tt.failures {
					return failure
				}
				return nil
			})

			err := handler(ctx, kafka.Message{Topic: "object-read"})

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantCalls, calls)
		})
	}
}
