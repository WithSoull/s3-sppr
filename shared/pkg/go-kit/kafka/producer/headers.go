package producer

import (
	"context"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type headersKey struct{}
type receiptKey struct{}

// WithHeaders attaches Kafka headers without changing the legacy Producer API.
func WithHeaders(ctx context.Context, h ...sarama.RecordHeader) context.Context {
	return context.WithValue(ctx, headersKey{}, h)
}
func headers(ctx context.Context) []sarama.RecordHeader {
	h, _ := ctx.Value(headersKey{}).([]sarama.RecordHeader)
	return h
}

// WithReceipt is only for shutdown barriers, never for HTTP telemetry.
func WithReceipt(ctx context.Context, ch chan error) context.Context {
	return context.WithValue(ctx, receiptKey{}, ch)
}
func receipt(ctx context.Context) any { ch, _ := ctx.Value(receiptKey{}).(chan error); return ch }
func report(msg *sarama.ProducerMessage, err error) {
	if ch, ok := msg.Metadata.(chan error); ok && ch != nil {
		ch <- err
	}
}

// LogResults drains both channels. Successes must be enabled on the producer.
func LogResults(ap sarama.AsyncProducer, logger Logger, onError func(string)) {
	successes, failures := ap.Successes(), ap.Errors()
	for successes != nil || failures != nil {
		select {
		case msg, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			report(msg, nil)
		case failure, ok := <-failures:
			if !ok {
				failures = nil
				continue
			}
			report(failure.Msg, failure.Err)
			logger.Error(context.Background(), "Failed to deliver message", zap.String("topic", failure.Msg.Topic), zap.Error(failure.Err))
			onError(failure.Msg.Topic)
		}
	}
}
