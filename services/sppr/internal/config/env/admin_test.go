package env

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAdminConfig(t *testing.T) {
	cases := []struct {
		name    string
		planner bool
		env     map[string]string
		enabled bool
		err     string
	}{
		{name: "migration profile default", enabled: false},
		{name: "planner requires addresses", planner: true, err: "AUTH_GRPC_ADDR"},
		{name: "planner default enabled", planner: true, env: map[string]string{"AUTH_GRPC_ADDR": "auth:50050", "AUTHZ_GRPC_ADDR": "authz:50051"}, enabled: true},
		{name: "explicit local opt out", planner: true, env: map[string]string{"ADMIN_AUTH_ENABLED": "false"}},
		{name: "migration opt in", env: map[string]string{"ADMIN_AUTH_ENABLED": "true", "AUTH_GRPC_ADDR": "auth:50050", "AUTHZ_GRPC_ADDR": "authz:50051"}, enabled: true},
		{name: "timeout", env: map[string]string{"AUTH_RPC_TIMEOUT": "0s"}, err: "positive"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"ADMIN_AUTH_ENABLED", "AUTH_GRPC_ADDR", "AUTHZ_GRPC_ADDR", "AUTH_RPC_TIMEOUT", "AUTHZ_RPC_TIMEOUT"} {
				t.Setenv(k, "")
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			cfg, err := NewAdminConfig(tt.planner)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.enabled, cfg.Enabled())
		})
	}
}
