package access

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/config"
	access "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/events/access"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/consumer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type queuedEvent struct {
	ctx     context.Context
	payload []byte
}

type Publisher struct {
	mu                  sync.Mutex
	cfg                 config.AccessConfig
	producer            kafka.Producer
	log                 consumer.Logger
	gatewayID, streamID string
	sequence            int64
	lastTime            time.Time
	queue               chan queuedEvent
	closed              bool
	done                chan struct{}
	cancel              context.CancelFunc
	pending             atomic.Int64
	dropped             atomic.Int64
	counter             metric.Int64Counter
}

func NewPublisher(cfg config.AccessConfig, producer kafka.Producer, log consumer.Logger, streamID string) (*Publisher, error) {
	counter, err := otel.Meter("gateway.access").Int64Counter("gateway_access_events_total")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Publisher{cfg: cfg, producer: producer, log: log, gatewayID: cfg.GatewayID(), streamID: streamID, queue: make(chan queuedEvent, cfg.QueueCapacity()), done: make(chan struct{}), cancel: cancel, counter: counter}
	go p.run(ctx)
	return p, nil
}

func (p *Publisher) StreamKey() string { return p.gatewayID + "/" + p.streamID }

func (p *Publisher) observe(ctx context.Context, outcome, reason string) {
	p.counter.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome), attribute.String("reason", reason)))
}

func (p *Publisher) drop(ctx context.Context, reason string) {
	p.observe(ctx, "dropped", reason)
	count := p.dropped.Add(1)
	// Report increasing totals without one log per dropped request under overload.
	if count&(count-1) == 0 {
		p.log.Error(ctx, "Access event dropped", zap.String("reason", reason), zap.Int64("dropped_total", count))
	}
}

func (p *Publisher) Record(ctx context.Context, event access.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		p.drop(ctx, "shutdown")
		return
	}
	p.sequence++
	now := time.Now().UTC().Truncate(time.Microsecond)
	if now.Before(p.lastTime) {
		now = p.lastTime
	}
	p.lastTime = now
	event.SchemaVersion = access.SchemaVersion
	event.GatewayID = p.gatewayID
	event.StreamID = p.streamID
	event.Sequence = p.sequence
	event.OccurredAt = now
	if err := access.Validate(event); err != nil {
		p.drop(ctx, "invalid")
		return
	}
	payload, err := json.Marshal(event)
	if err != nil {
		p.drop(ctx, "invalid")
		return
	}
	// Retain only trace identity, not a cancelled request or its values/body.
	deliveryCtx := trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx))
	p.pending.Add(1)
	select {
	case p.queue <- queuedEvent{ctx: deliveryCtx, payload: payload}:
		p.observe(ctx, "queued", "")
	default:
		p.pending.Add(-1)
		p.drop(ctx, "queue_full")
	}
}

func (p *Publisher) run(ctx context.Context) {
	defer close(p.done)
	for item := range p.queue {
		for {
			if ctx.Err() != nil {
				p.discard(item)
				return
			}
			attempt, cancel := context.WithTimeout(item.ctx, p.cfg.SendTimeout())
			stop := context.AfterFunc(ctx, cancel)
			err := p.producer.Send(attempt, []byte(p.StreamKey()), item.payload, nil)
			stop()
			cancel()
			if err == nil {
				p.pending.Add(-1)
				p.observe(item.ctx, "sent", "")
				break
			}
			if ctx.Err() != nil {
				p.discard(item)
				return
			}
			p.observe(item.ctx, "retry", "")
			p.log.Error(item.ctx, "Access event delivery failed; retrying in order", zap.Error(err))
			timer := time.NewTimer(p.cfg.RetryInterval())
			select {
			case <-ctx.Done():
				timer.Stop()
				p.discard(item)
				return
			case <-timer.C:
			}
		}
	}
}

func (p *Publisher) discard(item queuedEvent) {
	p.pending.Add(-1)
	p.drop(item.ctx, "shutdown")
	for item = range p.queue {
		p.pending.Add(-1)
		p.drop(item.ctx, "shutdown")
	}
}

func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	select {
	case <-p.done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.log.Error(context.Background(), "Access event drain timed out", zap.Int64("pending_events", p.pending.Load()), zap.Error(ctx.Err()))
		p.cancel()
		<-p.done
		return ctx.Err()
	}
}
