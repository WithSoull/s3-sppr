//go:build integration

package event_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository/history"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/stretchr/testify/require"
)

func TestLostStreamResume(t *testing.T) {
	for _, tt := range []struct {
		name             string
		heartbeat        bool
		sequence         int64
		unknown, missing int64
	}{
		{name: "contiguous event", sequence: 2},
		{name: "contiguous heartbeat", heartbeat: true, sequence: 1},
		{name: "event jump", sequence: 4, unknown: 1, missing: 2},
		{name: "heartbeat jump", heartbeat: true, sequence: 4, unknown: 1, missing: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, s := database(t)
			ctx := context.Background()
			e := accessEvent(1)
			deliver(t, s, e, 0)
			h := model.StreamHeartbeat{GatewayID: e.GatewayID, StreamID: e.StreamID, Sequence: 1, At: e.OccurredAt.Add(time.Second), Payload: []byte("before loss")}
			_, err := s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 1})
			require.NoError(t, err)
			lost := h.At.Add(31 * time.Second)
			require.NoError(t, s.CheckLostStreams(ctx, lost, 30*time.Second))
			require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE is_lost"))
			// Older receipts cannot revive a lost stream or erase its evidence.
			require.True(t, deliver(t, s, e, 0).Duplicate)
			replay, err := s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 1})
			require.NoError(t, err)
			require.True(t, replay.Duplicate)
			require.Equal(t, int64(0), scalar(t, client, "SELECT resumed_count FROM ingestion_streams"))
			e.Sequence = tt.sequence
			e.OccurredAt = lost.Add(time.Second)
			h.Sequence = tt.sequence
			h.At = e.OccurredAt
			h.Payload = []byte("resume")
			apply := func(offset int64) (model.IngestResult, error) {
				d := model.Delivery{Topic: "access", Offset: offset}
				if tt.heartbeat {
					return s.Heartbeat(ctx, h, d)
				}
				return ingest(s, e, d)
			}
			result, err := apply(2)
			require.NoError(t, err)
			require.Equal(t, model.IngestResult{Resumed: true, Missing: tt.missing}, result)
			require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_streams WHERE closed_at IS NULL AND lost_at IS NULL AND resumed_count=1 AND last_resumed_at IS NOT NULL"))
			require.Equal(t, tt.unknown, scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE is_lost AND last_sequence IS NULL"))
			require.Equal(t, tt.missing, scalar(t, client, "SELECT COALESCE(sum(last_sequence-first_sequence+1),0)::bigint FROM ingestion_gaps WHERE NOT is_lost"))
			// Same delivery and a later Kafka delivery of the same payload are both no-ops.
			for _, offset := range []int64{2, 3} {
				result, err = apply(offset)
				require.NoError(t, err)
				require.Equal(t, model.IngestResult{Duplicate: true}, result)
			}
			require.Equal(t, int64(1), scalar(t, client, "SELECT resumed_count FROM ingestion_streams"))
			demand := int64(2)
			if tt.heartbeat {
				demand = 1
			}
			require.Equal(t, demand, scalar(t, client, "SELECT sum(reads) FROM read_aggregates"))
			c, err := history.NewRepository(client).Coverage(ctx, lost.Add(-time.Hour))
			require.NoError(t, err)
			require.Equal(t, int64(1), c.OpenStreams)
			require.Zero(t, c.LostStreams)
			require.Zero(t, c.ClosedStreams)
			require.Equal(t, int64(1), c.ResumedCount)
			if !tt.heartbeat {
				require.Equal(t, lost.Add(-31*time.Second), c.Watermark)
			}
			h.At = e.OccurredAt.Add(time.Second)
			h.Payload = []byte("after resume")
			_, err = s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 4})
			require.NoError(t, err)
			c, err = history.NewRepository(client).Coverage(ctx, lost.Add(-time.Hour))
			require.NoError(t, err)
			if tt.heartbeat && tt.missing > 0 {
				require.Equal(t, accessEvent(1).OccurredAt, c.Watermark, "missing tail keeps the previous event barrier")
			} else {
				require.True(t, c.Watermark.After(lost.Add(-31*time.Second)))
			}
			if tt.missing == 0 {
				require.Equal(t, h.At, c.Watermark)
				require.Empty(t, c.Gaps)
			}
		})
	}
}

func TestClosedStreamRejectsNewMessages(t *testing.T) {
	for _, tt := range []struct {
		name string
		lost bool
	}{{name: "graceful closing"}, {name: "regression after loss", lost: true}} {
		t.Run(tt.name, func(t *testing.T) {
			client, s := database(t)
			ctx := context.Background()
			e := accessEvent(3)
			deliver(t, s, e, 0)
			h := model.StreamHeartbeat{GatewayID: e.GatewayID, StreamID: e.StreamID, Sequence: 3, At: e.OccurredAt.Add(time.Second), Closing: !tt.lost, Payload: []byte("barrier")}
			_, err := s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 1})
			require.NoError(t, err)
			if tt.lost {
				require.NoError(t, s.CheckLostStreams(ctx, h.At.Add(time.Minute), 30*time.Second))
				e.Sequence = 2
				h.Sequence = 2
			} else {
				e.Sequence = 4
				h.Sequence = 4
			}
			e.OccurredAt = h.At.Add(time.Minute)
			_, err = ingest(s, e, model.Delivery{Topic: "access", Offset: 2})
			require.ErrorIs(t, err, model.ErrRejected)
			h.At = e.OccurredAt
			h.Payload = []byte("new barrier")
			_, err = s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 3})
			require.ErrorIs(t, err, model.ErrRejected)
			require.Equal(t, int64(0), scalar(t, client, "SELECT resumed_count FROM ingestion_streams"))
			require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM access_events"))
		})
	}
}

func TestResumeRollsBackWithNormalProcessing(t *testing.T) {
	for _, tt := range []struct {
		name, constraint string
		heartbeat        bool
	}{
		{name: "event demand fails", constraint: `ALTER TABLE read_aggregates ADD CONSTRAINT test_fail CHECK(reads<=1)`},
		{name: "heartbeat receipt fails", heartbeat: true, constraint: `ALTER TABLE ingestion_heartbeats ADD CONSTRAINT test_fail CHECK(false) NOT VALID`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, s := database(t)
			ctx := context.Background()
			e := accessEvent(1)
			deliver(t, s, e, 0)
			h := model.StreamHeartbeat{GatewayID: e.GatewayID, StreamID: e.StreamID, Sequence: 1, At: e.OccurredAt.Add(time.Second), Payload: []byte("before")}
			_, err := s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 1})
			require.NoError(t, err)
			require.NoError(t, s.CheckLostStreams(ctx, h.At.Add(time.Minute), 30*time.Second))
			_, err = client.DB().ExecContext(ctx, db.Query{Name: "test:fail", QueryRaw: tt.constraint})
			require.NoError(t, err)
			e.Sequence = 2
			e.OccurredAt = h.At.Add(2 * time.Minute)
			h.At = e.OccurredAt
			h.Payload = []byte("resume")
			apply := func() (model.IngestResult, error) {
				if tt.heartbeat {
					return s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 2})
				}
				return ingest(s, e, model.Delivery{Topic: "access", Offset: 2})
			}
			_, err = apply()
			require.Error(t, err)
			require.NotErrorIs(t, err, model.ErrRejected)
			require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_streams WHERE lost_at IS NOT NULL AND closed_at=lost_at AND resumed_count=0 AND last_resumed_at IS NULL"))
			require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE is_lost"))
			table := "read_aggregates"
			if tt.heartbeat {
				table = "ingestion_heartbeats"
			}
			_, err = client.DB().ExecContext(ctx, db.Query{Name: "test:unblock", QueryRaw: "ALTER TABLE " + table + " DROP CONSTRAINT test_fail"})
			require.NoError(t, err)
			result, err := apply()
			require.NoError(t, err)
			require.True(t, result.Resumed)
			require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
		})
	}
}

func TestResumeMigrationDownPreservesConflictingGaps(t *testing.T) {
	client, s := database(t)
	ctx := context.Background()
	deliver(t, s, accessEvent(1), 0)
	require.NoError(t, s.CheckLostStreams(ctx, time.Now().Add(time.Hour), 30*time.Second))
	deliver(t, s, accessEvent(3), 1)
	down, err := os.ReadFile("../../../migrations/20261102000000_lost_stream_resume.down.sql")
	require.NoError(t, err)
	_, err = client.DB().ExecContext(ctx, db.Query{Name: "test:downgrade", QueryRaw: string(down)})
	require.ErrorContains(t, err, "cannot downgrade")
	require.Equal(t, int64(2), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
}

func TestRepeatedLossResolvesOnlyCurrentGap(t *testing.T) {
	client, s := database(t)
	ctx := context.Background()
	e := accessEvent(1)
	deliver(t, s, e, 0)
	h := model.StreamHeartbeat{GatewayID: e.GatewayID, StreamID: e.StreamID, Sequence: 1, At: e.OccurredAt.Add(time.Second), Payload: []byte("initial")}
	_, err := s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 1})
	require.NoError(t, err)
	lost := h.At.Add(time.Minute)
	require.NoError(t, s.CheckLostStreams(ctx, lost, 30*time.Second))
	e.Sequence = 3
	e.OccurredAt = lost.Add(time.Second)
	require.True(t, deliver(t, s, e, 2).Resumed)
	h.Sequence = 3
	h.At = e.OccurredAt.Add(time.Second)
	h.Payload = []byte("after jump")
	_, err = s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 3})
	require.NoError(t, err)
	require.NoError(t, s.CheckLostStreams(ctx, h.At.Add(time.Minute), 30*time.Second))
	require.Equal(t, int64(2), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE is_lost"))
	h.At = h.At.Add(2 * time.Minute)
	h.Payload = []byte("contiguous resume")
	result, err := s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 4})
	require.NoError(t, err)
	require.True(t, result.Resumed)
	require.Equal(t, int64(2), scalar(t, client, "SELECT resumed_count FROM ingestion_streams"))
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE is_lost AND first_sequence=2"))
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE NOT is_lost AND first_sequence=2 AND last_sequence=2"))
	require.Equal(t, int64(2), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
}

func TestEmptyLostStreamResumesWithHeartbeat(t *testing.T) {
	for _, closing := range []bool{false, true} {
		name := "open"
		if closing {
			name = "closing"
		}
		t.Run(name, func(t *testing.T) {
			client, s := database(t)
			ctx := context.Background()
			h := model.StreamHeartbeat{GatewayID: "gw", StreamID: "empty", At: accessEvent(1).OccurredAt, Payload: []byte("empty")}
			_, err := s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 0})
			require.NoError(t, err)
			require.NoError(t, s.CheckLostStreams(ctx, h.At.Add(time.Minute), 30*time.Second))
			h.At = h.At.Add(2 * time.Minute)
			h.Closing = closing
			h.Payload = []byte("empty resume")
			result, err := s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 1})
			require.NoError(t, err)
			require.True(t, result.Resumed)
			require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
			c, err := history.NewRepository(client).Coverage(ctx, h.At.Add(-time.Hour))
			require.NoError(t, err)
			require.Equal(t, h.At, c.Watermark)
			require.Equal(t, int64(1), c.ResumedCount)
			require.Zero(t, c.LostStreams)
			if closing {
				require.Equal(t, int64(1), c.ClosedStreams)
			} else {
				require.Equal(t, int64(1), c.OpenStreams)
			}
		})
	}
}

func TestResumeMigrationBackfillsExistingLostGap(t *testing.T) {
	client, s := database(t)
	ctx := context.Background()
	e := accessEvent(1)
	deliver(t, s, e, 0)
	require.NoError(t, s.CheckLostStreams(ctx, time.Now().Add(time.Hour), 30*time.Second))
	// Downgrade leaves the pre-existing unknown-size gap in the previous schema.
	for _, direction := range []string{"down", "up"} {
		sql, err := os.ReadFile("../../../migrations/20261102000000_lost_stream_resume." + direction + ".sql")
		require.NoError(t, err)
		_, err = client.DB().ExecContext(ctx, db.Query{Name: "test:migrate_resume", QueryRaw: string(sql)})
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE is_lost AND last_sequence IS NULL"))
	require.True(t, deliver(t, s, accessEvent(2), 1).Resumed)
	require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
	require.Equal(t, int64(2), scalar(t, client, "SELECT sum(reads) FROM read_aggregates"))
}
