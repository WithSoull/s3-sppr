package env

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTracingConfigIgnoresLoggerAndMetricsSettings(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SERVICE_NAME", "gateway-test")
	t.Setenv("OTEL_SERVICE_VERSION", "v1")
	t.Setenv("OTEL_ENVIRONMENT", "test")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4317")
	t.Setenv("LOGGER_AS_JSON", "invalid-bool")
	t.Setenv("OTEL_METRICS_PUSH_TIMEOUT", "invalid-duration")
	cfg, err := NewTracingConfig()
	require.NoError(t, err)
	require.True(t, cfg.Enabled())
	require.Equal(t, "gateway-test", cfg.ServiceName())
	require.Equal(t, "v1", cfg.ServiceVersion())
	require.Equal(t, "test", cfg.Environment())
	require.Equal(t, "collector:4317", cfg.CollectorEndpoint())
}
