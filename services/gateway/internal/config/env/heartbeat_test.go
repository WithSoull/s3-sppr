package env

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestHeartbeatInterval(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  time.Duration
		bad   bool
	}{{"", 5 * time.Second, false}, {"250ms", 250 * time.Millisecond, false}, {"0s", 0, true}, {"-1s", 0, true}, {"bad", 0, true}} {
		t.Run(tt.value, func(t *testing.T) {
			t.Setenv("ACCESS_EVENTS_ENABLED", "false")
			t.Setenv("ACCESS_HEARTBEAT_INTERVAL", tt.value)
			if tt.value == "" {
				require.NoError(t, os.Unsetenv("ACCESS_HEARTBEAT_INTERVAL"))
			}
			c, err := NewAccessEventsConfig()
			if tt.bad {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, c.HeartbeatInterval())
			}
		})
	}
}
