package observability

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMigrationMetrics(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(ctx)
	m, err := NewMigrationMetrics(provider.Meter("test"))
	require.NoError(t, err)
	defer m.Close()
	remaining := int64(100)
	require.NoError(t, m.RegisterBudget(func(context.Context) (model.BudgetStatus, error) {
		return model.BudgetStatus{Remaining: remaining}, nil
	}))
	m.Actions(ctx, "failed", "verify_mismatch", 1)
	m.Actions(ctx, "cancelled", "dependency_failed", 2)
	m.Copied(ctx, "HOT", 37)
	m.Copied(ctx, "HOT", 0)
	m.Cleanup(ctx, "delete_failed")
	m.Cleanup(ctx, "deleted")
	m.Duration(ctx, 2*time.Second)
	collect := func() map[string]metricdata.Metrics {
		var resource metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(ctx, &resource))
		metrics := map[string]metricdata.Metrics{}
		for _, scope := range resource.ScopeMetrics {
			for _, metric := range scope.Metrics {
				metrics[metric.Name] = metric
			}
		}
		return metrics
	}
	metrics := collect()
	require.Len(t, metrics, 5)
	actions := metrics["sppr_migration_actions_total"].Data.(metricdata.Sum[int64])
	require.Len(t, actions.DataPoints, 2)
	counts := map[string]int64{}
	for _, p := range actions.DataPoints {
		attrs := p.Attributes.ToSlice()
		require.Len(t, attrs, 2)
		for _, a := range attrs {
			require.Contains(t, []string{"outcome", "reason"}, string(a.Key))
		}
		outcome, _ := p.Attributes.Value("outcome")
		counts[outcome.AsString()] = p.Value
	}
	require.Equal(t, map[string]int64{"failed": 1, "cancelled": 2}, counts)
	copied := metrics["sppr_migration_bytes_copied_total"].Data.(metricdata.Sum[int64])
	require.Len(t, copied.DataPoints, 1)
	require.Equal(t, int64(37), copied.DataPoints[0].Value)
	class, _ := copied.DataPoints[0].Attributes.Value("target_class")
	require.Equal(t, "HOT", class.AsString())
	cleanups := metrics["sppr_migration_cleanups_total"].Data.(metricdata.Sum[int64])
	require.Len(t, cleanups.DataPoints, 2)
	duration := metrics["sppr_migration_duration_seconds"].Data.(metricdata.Histogram[float64])
	require.Equal(t, uint64(1), duration.DataPoints[0].Count)
	require.Equal(t, float64(2), duration.DataPoints[0].Sum)
	gauge := metrics["sppr_migration_budget_remaining_bytes"].Data.(metricdata.Gauge[int64])
	require.Equal(t, int64(100), gauge.DataPoints[0].Value)
	remaining = 200
	metrics = collect()
	require.Equal(t, int64(200), metrics["sppr_migration_budget_remaining_bytes"].Data.(metricdata.Gauge[int64]).DataPoints[0].Value)
}
