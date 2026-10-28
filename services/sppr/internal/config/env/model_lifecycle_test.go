package env

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestModelLifecycleConfig(t *testing.T) {
	c, e := NewPlannerConfig()
	require.NoError(t, e)
	require.Equal(t, 72*time.Hour, c.ModelMaxAge())
	require.Equal(t, 24*time.Hour, c.ModelUpdateEvery())
	require.Equal(t, 26*time.Hour, c.ModelMinHistory())
	require.Equal(t, 7*24*time.Hour, c.ModelTrainWindow())
	require.Equal(t, 30*time.Minute, c.ModelTrainTimeout())
	require.Contains(t, c.TrainCommand(), "{params}")
	for _, key := range []string{"PLANNER_MODEL_MAX_AGE", "PLANNER_MODEL_UPDATE_EVERY", "PLANNER_MODEL_MIN_HISTORY", "PLANNER_MODEL_TRAIN_WINDOW", "PLANNER_MODEL_TRAIN_TIMEOUT"} {
		t.Run(key, func(t *testing.T) { t.Setenv(key, "0s"); _, e := NewPlannerConfig(); require.Error(t, e) })
	}
}
