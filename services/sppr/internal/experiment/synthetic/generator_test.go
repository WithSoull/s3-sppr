package synthetic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func config() Config {
	return Config{Scenario: "stable", Seed: 0, Start: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Objects: 100, SizeBytes: 1048576, ReadsPerSecond: 1, SnapshotSeconds: 300, HotFraction: 0.1, HotProbability: 0.8, DecayHours: 24, PeriodHours: 24, BurstAtHours: 100, BurstDurationHours: 0.5, ShiftEveryHours: 24}
}

func TestNextReproducible(t *testing.T) {
	for _, name := range Scenarios {
		t.Run(name, func(t *testing.T) {
			c := config()
			c.Scenario = name
			a, err := New(c)
			require.NoError(t, err)
			b, err := New(c)
			require.NoError(t, err)
			other := c
			other.Seed = 1
			different, err := New(other)
			require.NoError(t, err)
			matches := 0
			for i := 0; i < 200; i++ {
				x, ok := a.Next()
				require.True(t, ok)
				y, ok := b.Next()
				require.True(t, ok)
				require.Equal(t, x, y)
				z, _ := different.Next()
				if x.VersionID == z.VersionID {
					matches++
				}
				require.Equal(t, c.Start.Add(time.Duration(i)*time.Second), x.OccurredAt)
			}
			require.Less(t, matches, 100)
		})
	}
}

func TestNextScenarioDistributions(t *testing.T) {
	cases := []struct {
		name, scenario string
		hour           float64
		group          int
		min, max       float64
	}{
		{"stable hot", "stable", 0, 0, 0.75, 0.9},
		{"decayed interest", "decay", 120, 0, 0.05, 0.2},
		{"periodic peak", "periodic", 24, 0, 0.75, 0.9},
		{"periodic trough", "periodic", 12, 0, 0.05, 0.2},
		{"burst before", "burst", 99, 0, 0.05, 0.2},
		{"burst during", "burst", 100, 0, 0.75, 0.9},
		{"shifted set", "shift", 25, 1, 0.75, 0.9},
		{"uniform", "uniform", 0, 0, 0.05, 0.2},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := config()
			c.Scenario = tt.scenario
			g, err := New(c)
			require.NoError(t, err)
			g.sequence = int64(tt.hour * 3600)
			hot := map[string]bool{}
			for _, i := range g.order[tt.group*g.hot : (tt.group+1)*g.hot] {
				hot[g.catalog[i].VersionID] = true
			}
			n := 0
			for i := 0; i < 1000; i++ {
				e, ok := g.Next()
				require.True(t, ok)
				if hot[e.VersionID] {
					n++
				}
			}
			fraction := float64(n) / 1000
			require.Greater(t, fraction, tt.min)
			require.Less(t, fraction, tt.max)
		})
	}
}

func TestNextLifecycleAndEnd(t *testing.T) {
	c := config()
	c.Lifecycle = true
	g, err := New(c)
	require.NoError(t, err)
	for _, hours := range []int64{0, 25, 100, 167} {
		g.sequence = hours * 3600
		for i := 0; i < 200; i++ {
			e, ok := g.Next()
			require.True(t, ok)
			found := false
			for _, v := range g.Catalog() {
				if e.VersionID == v.VersionID {
					require.True(t, v.Active(e.OccurredAt))
					found = true
				}
			}
			require.True(t, found)
		}
	}
	g.sequence = 168 * 3600
	_, ok := g.Next()
	require.False(t, ok)
	c.Scenario = "unknown"
	_, err = New(c)
	require.Error(t, err)
}
