package observability

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/metric"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
)

type Metrics struct {
	events   api.Int64Counter
	missing  api.Int64Counter
	duration api.Float64Histogram
}

func NewMetrics() (*Metrics, error) {
	m := &Metrics{}
	var err error
	m.events, err = metric.Meter().Int64Counter("sppr_ingestion_events_total")
	if err != nil {
		return nil, err
	}
	m.missing, err = metric.Meter().Int64Counter("sppr_ingestion_missing_events_total")
	if err != nil {
		return nil, err
	}
	m.duration, err = metric.Meter().Float64Histogram("sppr_ingestion_duration_seconds", api.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Metrics) Observe(ctx context.Context, outcome string, missing int64, elapsed time.Duration) {
	attrs := api.WithAttributes(attribute.String("outcome", outcome))
	m.events.Add(ctx, 1, attrs)
	m.duration.Record(ctx, elapsed.Seconds(), attrs)
	if missing > 0 {
		m.missing.Add(ctx, missing)
	}
}
