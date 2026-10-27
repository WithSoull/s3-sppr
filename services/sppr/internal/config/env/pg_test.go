package env

import (
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func TestPGConfigPreservesConnectionParameters(t *testing.T) {
	for _, tt := range []struct {
		name, host, user, password, database string
	}{
		{name: "ordinary values", host: "localhost", user: "sppr", password: "secret", database: "sppr"},
		{name: "reserved characters", host: "localhost", user: "user name@host", password: `space ' slash\ / ?&=`, database: "db name"},
		{name: "IPv6", host: "::1", user: "sppr", password: "secret", database: "sppr"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range map[string]string{
				"PG_DSN": "", "PG_HOST": tt.host, "PG_PORT_INNER": "5432",
				"PG_USER": tt.user, "PG_PASSWORD": tt.password, "PG_DATABASE_NAME": tt.database,
				"PG_SSL_MODE": "disable",
			} {
				t.Setenv(key, value)
			}
			cfg, err := NewPGConfig()
			require.NoError(t, err)
			parsed, err := pgx.ParseConfig(cfg.DSN())
			require.NoError(t, err)
			require.Equal(t, tt.host, parsed.Host)
			require.Equal(t, tt.user, parsed.User)
			require.Equal(t, tt.password, parsed.Password)
			require.Equal(t, tt.database, parsed.Database)
		})
	}
}
