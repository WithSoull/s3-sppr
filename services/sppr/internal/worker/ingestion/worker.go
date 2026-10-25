package ingestion

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/consumer"
	"go.uber.org/zap"
)

type Config interface {
	AccessTopic() string
	RetryInterval() time.Duration
	ProcessingTimeout() time.Duration
}

type Worker struct {
	group    sarama.ConsumerGroup
	consumer kafka.Consumer
	handler  kafka.MessageHandler
	cfg      Config
	log      consumer.Logger
	sessions atomic.Int64
	blocked  atomic.Int64
}

func NewWorker(group sarama.ConsumerGroup, handler kafka.MessageHandler, cfg Config, log consumer.Logger) *Worker {
	w := &Worker{group: group, handler: handler, cfg: cfg, log: log}
	w.consumer = consumer.NewConsumer(&observedGroup{ConsumerGroup: group, worker: w}, []string{cfg.AccessTopic()}, log, w.retry)
	return w
}

func (w *Worker) Ready() bool { return w.sessions.Load() > 0 && w.blocked.Load() == 0 }

func (w *Worker) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-w.group.Errors():
				if !ok {
					return
				}
				w.log.Error(ctx, "Kafka consumer group error", zap.Error(err))
			}
		}
	}()
	defer func() { cancel(); wg.Wait() }()
	for ctx.Err() == nil {
		err := w.consumer.Consume(ctx, w.handler)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return errors.New("Kafka consumer stopped unexpectedly")
		}
		w.log.Error(ctx, "Kafka consumer restarting", zap.Error(err))
		if err := wait(ctx, w.cfg.RetryInterval()); err != nil {
			return nil
		}
	}
	return nil
}

// The shared consumer continues after a handler error. Retrying inside the
// handler prevents a later successful offset from committing past a failure.
func (w *Worker) retry(next kafka.MessageHandler) kafka.MessageHandler {
	return func(ctx context.Context, msg kafka.Message) error {
		blocked := false
		defer func() {
			if blocked {
				w.blocked.Add(-1)
			}
		}()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			attempt, cancel := context.WithTimeout(ctx, w.cfg.ProcessingTimeout())
			err := next(attempt, msg)
			cancel()
			if err == nil {
				return ctx.Err()
			}
			if !blocked {
				blocked = true
				w.blocked.Add(1)
			}
			w.log.Error(ctx, "Access event processing failed; partition blocked", zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition), zap.Int64("offset", msg.Offset), zap.Error(err))
			if err := wait(ctx, w.cfg.RetryInterval()); err != nil {
				return err
			}
		}
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type observedGroup struct {
	sarama.ConsumerGroup
	worker *Worker
}

func (g *observedGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return g.ConsumerGroup.Consume(ctx, topics, &sessionHandler{ConsumerGroupHandler: handler, worker: g.worker})
}

type sessionHandler struct {
	sarama.ConsumerGroupHandler
	worker *Worker
	active bool
}

func (h *sessionHandler) Setup(s sarama.ConsumerGroupSession) error {
	if err := h.ConsumerGroupHandler.Setup(s); err != nil {
		return err
	}
	if len(s.Claims()) > 0 {
		h.active = true
		h.worker.sessions.Add(1)
	}
	return nil
}
func (h *sessionHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	if h.active {
		h.worker.sessions.Add(-1)
		h.active = false
	}
	return h.ConsumerGroupHandler.Cleanup(s)
}
