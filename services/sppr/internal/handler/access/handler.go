package access

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Observer interface {
	Observe(context.Context, string, int64, time.Duration)
}

type Handler struct {
	ingestor service.Ingestor
	observer Observer
	tracer   trace.Tracer
	maxBytes int
}

func NewHandler(ingestor service.Ingestor, observer Observer, tracer trace.Tracer, maxBytes int) *Handler {
	return &Handler{ingestor: ingestor, observer: observer, tracer: tracer, maxBytes: maxBytes}
}

func (h *Handler) Handle(ctx context.Context, msg kafka.Message) (err error) {
	carrier := propagation.MapCarrier{}
	for key, value := range msg.Headers {
		carrier[key] = string(value)
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	ctx, span := h.tracer.Start(ctx, "access.ingest", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(
		attribute.String("messaging.destination.name", msg.Topic), attribute.Int64("messaging.kafka.offset", msg.Offset)))
	defer span.End()
	start := time.Now()
	outcome := "error"
	var missing int64
	defer func() {
		h.observer.Observe(ctx, outcome, missing, time.Since(start))
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "ingestion failed")
		}
	}()
	if len(msg.Value) > h.maxBytes || !utf8.Valid(msg.Value) {
		return fmt.Errorf("event exceeds size limit or contains invalid UTF-8")
	}
	var event model.AccessEvent
	decoder := json.NewDecoder(bytes.NewReader(msg.Value))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&event); err != nil {
		return fmt.Errorf("decode access event: %w", err)
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("access event must contain exactly one JSON value")
	}
	if !bytes.Equal(msg.Key, []byte(event.StreamKey())) {
		return fmt.Errorf("Kafka key must equal gateway_id/stream_id")
	}
	result, err := h.ingestor.Ingest(ctx, event, model.Delivery{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
	if err != nil {
		return err
	}
	outcome = "stored"
	if result.Duplicate {
		outcome = "duplicate"
	}
	missing = result.Missing
	return nil
}
