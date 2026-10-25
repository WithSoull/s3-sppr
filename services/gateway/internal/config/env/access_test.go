package env

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccessConfig(t *testing.T) {
	for _, tt := range []struct {
		name        string
		env         map[string]string
		wantErr     bool
		wantEnabled bool
	}{
		{name: "disabled standalone", env: map[string]string{"ACCESS_EVENTS_ENABLED": "false"}},
		{name: "enabled", wantEnabled: true},
		{name: "invalid ID", env: map[string]string{"GATEWAY_ID": "bad/id"}, wantErr: true},
		{name: "missing brokers", env: map[string]string{"KAFKA_BOOTSTRAP": ""}, wantErr: true},
		{name: "invalid broker", env: map[string]string{"KAFKA_BOOTSTRAP": "localhost"}, wantErr: true},
		{name: "dot topic", env: map[string]string{"KAFKA_ACCESS_TOPIC": "."}, wantErr: true},
		{name: "double dot topic", env: map[string]string{"KAFKA_ACCESS_TOPIC": ".."}, wantErr: true},
		{name: "punctuated topic", env: map[string]string{"KAFKA_ACCESS_TOPIC": "bad!"}, wantErr: true},
		{name: "non-numeric port", env: map[string]string{"KAFKA_BOOTSTRAP": "localhost:abc"}, wantErr: true},
		{name: "invalid topic", env: map[string]string{"KAFKA_ACCESS_TOPIC": "bad topic"}, wantErr: true},
		{name: "zero queue", env: map[string]string{"ACCESS_EVENTS_QUEUE_CAPACITY": "0"}, wantErr: true},
		{name: "excessive queue", env: map[string]string{"ACCESS_EVENTS_QUEUE_CAPACITY": "65537"}, wantErr: true},
		{name: "zero retry", env: map[string]string{"ACCESS_EVENTS_RETRY_INTERVAL": "0s"}, wantErr: true},
		{name: "excessive send timeout", env: map[string]string{"ACCESS_EVENTS_SEND_TIMEOUT": "31s"}, wantErr: true},
		{name: "zero shutdown", env: map[string]string{"ACCESS_EVENTS_SHUTDOWN_TIMEOUT": "0s"}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ACCESS_EVENTS_ENABLED", "true")
			t.Setenv("GATEWAY_ID", "gw-1")
			t.Setenv("KAFKA_BOOTSTRAP", " localhost:9092 , [::1]:9092 ")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			cfg, err := NewAccessConfig()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantEnabled, cfg.Enabled())
			if cfg.Enabled() {
				require.Equal(t, []string{"localhost:9092", "[::1]:9092"}, cfg.Brokers())
				require.Equal(t, "opens3.access.v1", cfg.AccessTopic())
				require.Equal(t, 1024, cfg.QueueCapacity())
			}
		})
	}
}
