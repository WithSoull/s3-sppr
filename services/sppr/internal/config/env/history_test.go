package env

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestHistoryDurations(t *testing.T) {
	for _, setting := range []struct {
		name         string
		defaultValue time.Duration
		get          func() (time.Duration, error)
	}{
		{"INGEST_STREAM_LOST_AFTER", 30 * time.Second, func() (time.Duration, error) {
			c, err := NewRetryConfig()
			if err != nil {
				return 0, err
			}
			return c.StreamLostAfter(), nil
		}},
		{"PLANNER_MAX_HISTORY_LAG", 60 * time.Second, func() (time.Duration, error) {
			c, err := NewPlannerConfig()
			if err != nil {
				return 0, err
			}
			return c.MaxHistoryLag(), nil
		}},
	} {
		for _, tt := range []struct {
			value string
			want  time.Duration
			bad   bool
		}{{"", setting.defaultValue, false}, {"250ms", 250 * time.Millisecond, false}, {"0s", 0, true}, {"-1s", 0, true}, {"bad", 0, true}} {
			t.Run(setting.name+"="+tt.value, func(t *testing.T) {
				t.Setenv(setting.name, tt.value)
				if tt.value == "" {
					require.NoError(t, os.Unsetenv(setting.name))
				}
				got, err := setting.get()
				if tt.bad {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, tt.want, got)
				}
			})
		}
	}
}
