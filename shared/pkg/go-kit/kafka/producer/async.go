package producer

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"go.uber.org/zap"
)

var ErrBufferFull = errors.New("kafka producer buffer is full")

// sarama's Input() is unbuffered: its dispatcher goroutine takes one message
// at a time and moves it into ChannelBufferSize buffers. A bare select/default
// fails whenever the dispatcher is between two messages, so Send waits for it
// briefly. The wait expires only when those buffers are full.
const handoffTimeout = time.Millisecond

type asyncProducer struct {
	asyncProducer sarama.AsyncProducer
	topic         string
}

// NewAsyncProducer implements kafka.Producer for one topic of a shared sarama.AsyncProducer.
func NewAsyncProducer(ap sarama.AsyncProducer, topic string) kafka.Producer {
	return &asyncProducer{
		asyncProducer: ap,
		topic:         topic,
	}
}

// Send hands the message to sarama and returns without waiting for Kafka:
// nil means sarama accepted it, ErrBufferFull means the event is lost.
// Delivery failures arrive later in Errors(), see LogErrors.
// Send keeps key and value until delivery, so callers must not reuse them.
func (p *asyncProducer) Send(_ context.Context, key, value []byte, _ kafka.PrettyDecoder) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	select {
	case p.asyncProducer.Input() <- msg:
		return nil
	default:
	}

	timer := time.NewTimer(handoffTimeout)
	defer timer.Stop()
	select {
	case p.asyncProducer.Input() <- msg:
		return nil
	case <-timer.C:
		return ErrBufferFull
	}
}

// LogErrors reads ap.Errors() until the producer is closed. Each error is a
// message lost after sarama's retries. Run it once per sarama.AsyncProducer.
func LogErrors(ap sarama.AsyncProducer, logger Logger, onError func(topic string)) {
	for pErr := range ap.Errors() {
		logger.Error(context.Background(), "Failed to deliver message",
			zap.String("topic", pErr.Msg.Topic),
			zap.Error(pErr.Err),
		)
		onError(pErr.Msg.Topic)
	}
}
