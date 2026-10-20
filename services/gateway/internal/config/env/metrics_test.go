package env

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMetricsConfig(t *testing.T) {
	tests := []struct {
		name, interval string
		want           time.Duration
		wantErr        bool
	}{
		{name: "valid", interval: "30s", want: 30 * time.Second},
		{name: "zero", interval: "0s", wantErr: true},
		{name: "negative", interval: "-1s", wantErr: true},
		{name: "malformed", interval: "bad", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_ENABLED", "false")
			t.Setenv("OTEL_SERVICE_NAME", "gateway")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4317")
			t.Setenv("OTEL_METRICS_PUSH_TIMEOUT", tt.interval)
			t.Setenv("LOGGER_AS_JSON", "invalid-bool")
			cfg, err := NewMetricsConfig()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.False(t, cfg.Enabled())
			require.Equal(t, tt.want, cfg.PushTimeout())
		})
	}
}
