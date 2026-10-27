package kafka

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/consumer"
)

// Retry repeats the handler until it succeeds or ctx is cancelled. The group
// handler skips MarkMessage on error and continues, so the next successful
// message would commit the offset past the failed one.
func Retry(interval time.Duration) consumer.Middleware {
	return func(next kafka.MessageHandler) kafka.MessageHandler {
		return func(ctx context.Context, msg kafka.Message) error {
			for {
				// After a rebalance the claim may still deliver messages; none of
				// them may succeed past a message that is being retried.
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := next(ctx, msg); err == nil {
					return nil
				}

				timer := time.NewTimer(interval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
}
