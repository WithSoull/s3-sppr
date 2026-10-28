package event

import (
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func TestHeartbeatProgress(t *testing.T) {
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	last := now.Add(-time.Second)
	old := last.Add(-time.Second)
	for _, tt := range []struct {
		name                   string
		seq                    int64
		last, hb, closed, lost *time.Time
		h                      model.StreamHeartbeat
		missing                int64
		complete               *time.Time
		bad                    bool
	}{
		{name: "zero event stream", h: model.StreamHeartbeat{At: now}, complete: &now},
		{name: "caught up", seq: 3, last: &last, h: model.StreamHeartbeat{Sequence: 3, At: now}, complete: &now},
		{name: "tail loss", seq: 3, last: &last, h: model.StreamHeartbeat{Sequence: 7, At: now}, missing: 4, complete: &last},
		{name: "all events lost", h: model.StreamHeartbeat{Sequence: 7, At: now}, missing: 7},
		{name: "backward sequence", seq: 3, h: model.StreamHeartbeat{Sequence: 2, At: now}, bad: true},
		{name: "event time regression", seq: 3, last: &last, h: model.StreamHeartbeat{Sequence: 3, At: old}, bad: true},
		{name: "heartbeat time regression", seq: 3, hb: &last, h: model.StreamHeartbeat{Sequence: 3, At: old}, bad: true},
		{name: "lost stream resumes contiguously", seq: 3, last: &last, closed: &now, lost: &now, h: model.StreamHeartbeat{Sequence: 3, At: now}, complete: &now},
		{name: "lost stream resumes with tail loss", seq: 3, last: &last, closed: &now, lost: &now, h: model.StreamHeartbeat{Sequence: 5, At: now}, missing: 2, complete: &last},
		{name: "lost stream still rejects regression", seq: 3, closed: &now, lost: &now, h: model.StreamHeartbeat{Sequence: 2, At: now}, bad: true},
		{name: "lost stream still rejects time regression", seq: 3, hb: &last, closed: &now, lost: &now, h: model.StreamHeartbeat{Sequence: 3, At: old}, bad: true},
		{name: "gracefully closed streams cannot reopen", closed: &last, h: model.StreamHeartbeat{At: now}, bad: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n, c, err := heartbeatProgress(tt.seq, tt.last, tt.hb, tt.closed, tt.lost, tt.h, 0)
			if tt.bad {
				require.ErrorIs(t, err, model.ErrRejected)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.missing, n)
				require.Equal(t, tt.complete, c)
			}
		})
	}
}

func TestHeartbeatDoesNotClaimMissingSequencesWereReceived(t *testing.T) {
	at := time.Now().UTC()
	last := at.Add(-time.Second)
	n, c, err := heartbeatProgress(3, &last, &last, nil, nil, model.StreamHeartbeat{Sequence: 3, At: at}, 2)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, &last, c)
}
