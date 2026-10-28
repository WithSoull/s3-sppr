package env

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A mistyped env tag silently leaves the default in place, so each group is checked by name.
func TestHTTPConfigReadsAddress(t *testing.T) {
	t.Setenv("GATEWAY_HTTP_ADDR", ":9090")
	cfg, err := NewHTTPConfig()
	require.NoError(t, err)
	require.Equal(t, ":9090", cfg.Address())
}

func TestStorageConfigReadsAddressPerClass(t *testing.T) {
	t.Setenv("STORAGE_HOT_GRPC_ADDR", "storage-hot:50053")
	t.Setenv("STORAGE_COLD_GRPC_ADDR", "storage-cold:50053")
	cfg, err := NewStorageConfig()
	require.NoError(t, err)
	require.Equal(t, "storage-hot:50053", cfg.HotAddress())
	require.Equal(t, "storage-cold:50053", cfg.ColdAddress())
}

func TestStorageConfigRetrieveChunkSize(t *testing.T) {
	tests := []struct {
		value   string
		want    int
		wantErr bool
	}{
		{value: "262144", want: 262144},
		{value: "1", want: 1},
		{value: "0", wantErr: true},
		{value: "-1", wantErr: true},
		{value: "67108865", wantErr: true}, // one above the 64 MiB limit
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			t.Setenv("STORAGE_RETRIEVE_CHUNK_SIZE_BYTES", tt.value)
			cfg, err := NewStorageConfig()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.RetrieveChunkSizeBytes())
		})
	}
}

// All durations share one rule: positive and parseable.
func TestDurationSettingsMustBePositive(t *testing.T) {
	settings := []struct {
		name  string
		setup func(t *testing.T, value string) (time.Duration, error)
	}{
		{"GATEWAY_MAX_READ_DURATION", func(t *testing.T, value string) (time.Duration, error) {
			t.Setenv("GATEWAY_MAX_READ_DURATION", value)
			cfg, err := NewHTTPConfig()
			if err != nil {
				return 0, err
			}
			return cfg.MaxReadDuration(), nil
		}},
		{"METADATA_RPC_TIMEOUT", func(t *testing.T, value string) (time.Duration, error) {
			t.Setenv("METADATA_RPC_TIMEOUT", value)
			cfg, err := NewMetadataConfig()
			if err != nil {
				return 0, err
			}
			return cfg.Timeout(), nil
		}},
		{"OTEL_METRICS_PUSH_TIMEOUT", func(t *testing.T, value string) (time.Duration, error) {
			t.Setenv("OTEL_ENABLED", "false")
			t.Setenv("OTEL_SERVICE_NAME", "gateway")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4317")
			t.Setenv("OTEL_METRICS_PUSH_TIMEOUT", value)
			cfg, err := NewMetricsConfig()
			if err != nil {
				return 0, err
			}
			return cfg.PushTimeout(), nil
		}},
	}
	for _, setting := range settings {
		for _, tt := range []struct {
			value   string
			want    time.Duration
			wantErr bool
		}{
			{value: "200ms", want: 200 * time.Millisecond},
			{value: "0s", wantErr: true},
			{value: "-1s", wantErr: true},
			{value: "bad", wantErr: true},
		} {
			t.Run(setting.name+"="+tt.value, func(t *testing.T) {
				got, err := setting.setup(t, tt.value)
				if tt.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			})
		}
	}
}

func TestAccessEventsConfig(t *testing.T) {
	tests := []struct {
		name        string
		env         map[string]string
		wantEnabled bool
		wantID      string
		wantErr     bool
	}{
		{name: "disabled without gateway id", env: map[string]string{"ACCESS_EVENTS_ENABLED": "false"}},
		{name: "enabled", env: map[string]string{"ACCESS_EVENTS_ENABLED": "true", "GATEWAY_ID": "gateway-1"}, wantEnabled: true, wantID: "gateway-1"},
		{name: "enabled requires gateway id", env: map[string]string{"ACCESS_EVENTS_ENABLED": "true"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GATEWAY_ID", "")
			for key, value := range tt.env {
				t.Setenv(key, value)
			}
			cfg, err := NewAccessEventsConfig()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantEnabled, cfg.Enabled())
			require.Equal(t, tt.wantID, cfg.GatewayID())
		})
	}
}

func TestKafkaProducerConfigReadsNames(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092")
	t.Setenv("KAFKA_OBJECT_READ_TOPIC", "reads")
	t.Setenv("KAFKA_OBJECT_WRITTEN_TOPIC", "writes")
	kafka, err := NewKafkaConfig()
	require.NoError(t, err)
	read, err := NewObjectReadProducerConfig()
	require.NoError(t, err)
	written, err := NewObjectWrittenProducerConfig()
	require.NoError(t, err)
	require.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, kafka.Brokers())
	require.Equal(t, "reads", read.Topic())
	require.Equal(t, "writes", written.Topic())
}

// The idempotent producer settings are validated by sarama itself.
func TestSaramaConfigIsValidIdempotentProducer(t *testing.T) {
	cfg, err := NewSaramaConfig()
	require.NoError(t, err)
	config := cfg.Config()
	require.NoError(t, config.Validate())
	require.True(t, config.Producer.Idempotent)
	require.True(t, config.Producer.Return.Successes)
	require.Equal(t, 1024, config.ChannelBufferSize)
}

// Logger, tracing and metrics read overlapping OTEL_*/LOGGER_* names, and a broken value meant
// for one group must not stop the others from starting.
func TestConfigsIgnoreUnrelatedSettings(t *testing.T) {
	t.Run("logger", func(t *testing.T) {
		t.Setenv("LOGGER_LEVEL", "debug")
		t.Setenv("LOGGER_AS_JSON", "true")
		t.Setenv("LOGGER_ENABLE_OLTP", "false")
		t.Setenv("OTEL_METRICS_PUSH_TIMEOUT", "invalid-duration")
		cfg, err := NewLoggerConfig()
		require.NoError(t, err)
		require.Equal(t, "debug", cfg.LogLevel())
		require.True(t, cfg.AsJSON())
		require.False(t, cfg.EnableOLTP())
	})
	t.Run("tracing", func(t *testing.T) {
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
	})
	t.Run("metrics", func(t *testing.T) {
		t.Setenv("OTEL_ENABLED", "false")
		t.Setenv("OTEL_SERVICE_NAME", "gateway")
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4317")
		t.Setenv("LOGGER_AS_JSON", "invalid-bool")
		cfg, err := NewMetricsConfig()
		require.NoError(t, err)
		require.False(t, cfg.Enabled())
	})
}
