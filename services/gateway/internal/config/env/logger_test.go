package env

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoggerConfigIgnoresMetricsSettings(t *testing.T) {
	t.Setenv("LOGGER_LEVEL", "debug")
	t.Setenv("LOGGER_AS_JSON", "true")
	t.Setenv("LOGGER_ENABLE_OLTP", "false")
	t.Setenv("OTEL_METRICS_PUSH_TIMEOUT", "invalid-duration")
	cfg, err := NewLoggerConfig()
	require.NoError(t, err)
	require.Equal(t, "debug", cfg.LogLevel())
	require.True(t, cfg.AsJSON())
	require.False(t, cfg.EnableOLTP())
}
