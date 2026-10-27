package env

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewMigrationConfig(t *testing.T) {
	cases := []struct {
		name       string
		env        map[string]string
		wantBudget int64
		wantErr    string
	}{
		{name: "invalid cleanup interval", env: map[string]string{"MIGRATION_CLEANUP_INTERVAL": "0s"}, wantErr: "MIGRATION_CLEANUP_INTERVAL"},
		{name: "defaults", wantBudget: 1 << 30},
		{name: "plain bytes", env: map[string]string{"MIGRATION_BUDGET_BYTES": "1500"}, wantBudget: 1500},
		{name: "MiB", env: map[string]string{"MIGRATION_BUDGET_BYTES": "512MiB"}, wantBudget: 512 << 20},
		{name: "unknown unit", env: map[string]string{"MIGRATION_BUDGET_BYTES": "1GB"}, wantErr: "invalid size \"1GB\""},
		{name: "zero budget", env: map[string]string{"MIGRATION_BUDGET_BYTES": "0"}, wantErr: "MIGRATION_BUDGET_BYTES"},
		{name: "cleanup delay equal to read limit", env: map[string]string{"MIGRATION_SOURCE_CLEANUP_DELAY": "5m", "GATEWAY_MAX_READ_DURATION": "5m"}, wantErr: "must exceed GATEWAY_MAX_READ_DURATION"},
		{name: "longer read limit", env: map[string]string{"GATEWAY_MAX_READ_DURATION": "20m"}, wantErr: "must exceed GATEWAY_MAX_READ_DURATION"},
		{name: "no workers", env: map[string]string{"MIGRATION_WORKERS": "0"}, wantErr: "MIGRATION_WORKERS"},
		{name: "no attempts", env: map[string]string{"MIGRATION_MAX_ATTEMPTS": "0"}, wantErr: "MIGRATION_MAX_ATTEMPTS"},
		{name: "short window", env: map[string]string{"MIGRATION_BUDGET_WINDOW": "500ms"}, wantErr: "MIGRATION_BUDGET_WINDOW"},
		{name: "negative interval", env: map[string]string{"MIGRATION_MIN_INTERVAL": "-1s"}, wantErr: "MIGRATION_MIN_INTERVAL"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.env {
				t.Setenv(key, value)
			}
			cfg, err := NewMigrationConfig()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantBudget, cfg.BudgetBytes())
			require.False(t, cfg.Enabled())
			require.Equal(t, 2, cfg.Workers())
			require.Equal(t, 10*time.Minute, cfg.SourceCleanupDelay())
		})
	}
}
