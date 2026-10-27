package env

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewCleanupConfig(t *testing.T) {
	tests := []struct {
		name         string
		cleanupDelay string
		maxRead      string
		wantDelay    time.Duration
		wantErr      bool
	}{
		{name: "defaults", wantDelay: 10 * time.Minute},
		{name: "delay above read limit", cleanupDelay: "2m", maxRead: "1m", wantDelay: 2 * time.Minute},
		{name: "delay equal to read limit", cleanupDelay: "5m", maxRead: "5m", wantErr: true},
		{name: "delay below default read limit", cleanupDelay: "1m", wantErr: true},
		{name: "non-positive read limit", cleanupDelay: "1m", maxRead: "0s", wantErr: true},
		{name: "malformed delay", cleanupDelay: "soon", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.cleanupDelay != "" {
				t.Setenv("METADATA_DELETE_CLEANUP_DELAY", tt.cleanupDelay)
			}
			if tt.maxRead != "" {
				t.Setenv("GATEWAY_MAX_READ_DURATION", tt.maxRead)
			}

			cfg, err := NewCleanupConfig()
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantDelay, cfg.DeleteCleanupDelay())
		})
	}
}
