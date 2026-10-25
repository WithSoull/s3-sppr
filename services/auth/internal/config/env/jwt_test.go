package env_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alesplll/opens3-rebac/services/auth/internal/config/env"
)

func TestNewJWTConfig_TokenTTL(t *testing.T) {
	tests := []struct {
		name        string
		ttlEnv      map[string]string
		wantRefresh time.Duration
		wantAccess  time.Duration
	}{
		{
			name:        "defaults apply when TTL variables are unset",
			wantRefresh: time.Hour,
			wantAccess:  15 * time.Minute,
		},
		{
			name: "explicit values override defaults",
			ttlEnv: map[string]string{
				"REFRESH_TOKEN_TTL": "24h",
				"ACCESS_TOKEN_TTL":  "30s",
			},
			wantRefresh: 24 * time.Hour,
			wantAccess:  30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("REFRESH_TOKEN_SECRET", "refresh_secret")
			t.Setenv("ACCESS_TOKEN_SECRET", "access_secret")
			for _, key := range []string{"REFRESH_TOKEN_TTL", "ACCESS_TOKEN_TTL"} {
				// t.Setenv registers the cleanup that restores the original value.
				t.Setenv(key, "")
				require.NoError(t, os.Unsetenv(key))
			}
			for key, value := range tt.ttlEnv {
				t.Setenv(key, value)
			}

			cfg, err := env.NewJWTConfig()

			require.NoError(t, err)
			require.Equal(t, tt.wantRefresh, cfg.RefreshTokenExpiration())
			require.Equal(t, tt.wantAccess, cfg.AccessTokenExpiration())
		})
	}
}
