package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCoverageWatermark(t *testing.T) {
	at := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	old := at.Add(-time.Hour)
	mid := at.Add(-time.Minute)
	future := at.Add(time.Hour)
	for _, tt := range []struct {
		name               string
		streams            []streamProgress
		known              bool
		w                  time.Time
		open, closed, lost int64
	}{
		{name: "no streams"},
		{name: "event alone is not completeness", streams: []streamProgress{{First: &old, Created: old}}, open: 1},
		{name: "minimum across open topics", streams: []streamProgress{{First: &old, Heartbeat: &at, Complete: &at}, {First: &old, Heartbeat: &mid, Complete: &mid}}, known: true, w: mid, open: 2},
		{name: "unknown open holds back known one", streams: []streamProgress{{First: &old, Heartbeat: &at, Complete: &at}, {First: &old}}, open: 2},
		{name: "closed does not hold open", streams: []streamProgress{{First: &old, Heartbeat: &at, Complete: &at}, {First: &old, Heartbeat: &mid, Complete: &mid, Closed: &mid}}, known: true, w: at, open: 1, closed: 1},
		{name: "closed maximum", streams: []streamProgress{{First: &old, Heartbeat: &at, Complete: &at, Closed: &at}, {First: &old, Heartbeat: &mid, Complete: &mid, Closed: &at}}, known: true, w: at, closed: 2},
		{name: "lost retains barrier", streams: []streamProgress{{First: &old, Heartbeat: &mid, Complete: &mid, Closed: &at, Lost: &at}}, known: true, w: mid, closed: 1, lost: 1},
		{name: "closed cap", streams: []streamProgress{{First: &old, Heartbeat: &future, Complete: &future, Closed: &at}}, known: true, w: at, closed: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := coverageOf(tt.streams)
			require.Equal(t, tt.known, c.Known)
			require.Equal(t, tt.w, c.Watermark)
			require.Equal(t, tt.open, c.OpenStreams)
			require.Equal(t, tt.closed, c.ClosedStreams)
			require.Equal(t, tt.lost, c.LostStreams)
		})
	}
}
