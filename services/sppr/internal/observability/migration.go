package observability

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
)

type MigrationMetrics struct {
	meter        api.Meter
	actions      api.Int64Counter
	copied       api.Int64Counter
	cleanups     api.Int64Counter
	duration     api.Float64Histogram
	budget       api.Int64ObservableGauge
	registration api.Registration
}

func NewMigrationMetrics(meter api.Meter) (*MigrationMetrics, error) {
	m := &MigrationMetrics{meter: meter}
	var err error
	m.actions, err = m.meter.Int64Counter("sppr_migration_actions_total")
	if err != nil {
		return nil, err
	}
	m.copied, err = m.meter.Int64Counter("sppr_migration_bytes_copied_total", api.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	m.cleanups, err = m.meter.Int64Counter("sppr_migration_cleanups_total")
	if err != nil {
		return nil, err
	}
	m.duration, err = m.meter.Float64Histogram("sppr_migration_duration_seconds", api.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	m.budget, err = m.meter.Int64ObservableGauge("sppr_migration_budget_remaining_bytes", api.WithUnit("By"))
	return m, err
}

func (m *MigrationMetrics) RegisterBudget(get func(context.Context) (model.BudgetStatus, error)) error {
	var err error
	m.registration, err = m.meter.RegisterCallback(func(ctx context.Context, o api.Observer) error {
		budget, err := get(ctx)
		if err != nil {
			return err
		}
		o.ObserveInt64(m.budget, budget.Remaining)
		return nil
	}, m.budget)
	return err
}

func (m *MigrationMetrics) Close() error {
	if m == nil || m.registration == nil {
		return nil
	}
	return m.registration.Unregister()
}

func (m *MigrationMetrics) Actions(ctx context.Context, outcome, reason string, count int64) {
	m.actions.Add(ctx, count, api.WithAttributes(attribute.String("outcome", outcome), attribute.String("reason", reason)))
}
func (m *MigrationMetrics) Copied(ctx context.Context, targetClass string, bytes int64) {
	if bytes > 0 {
		m.copied.Add(ctx, bytes, api.WithAttributes(attribute.String("target_class", targetClass)))
	}
}
func (m *MigrationMetrics) Duration(ctx context.Context, elapsed time.Duration) {
	m.duration.Record(ctx, elapsed.Seconds())
}
func (m *MigrationMetrics) Cleanup(ctx context.Context, outcome string) {
	m.cleanups.Add(ctx, 1, api.WithAttributes(attribute.String("outcome", outcome)))
}
