//go:build integration

package event_test

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	history "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/history"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/stretchr/testify/require"
)

func TestHeartbeatPersistenceReplayTailAndLoss(t *testing.T) {
	client, s := database(t)
	ctx := context.Background()
	e := accessEvent(1)
	deliver(t, s, e, 0)
	h := model.StreamHeartbeat{GatewayID: e.GatewayID, StreamID: e.StreamID, Sequence: 3, At: e.OccurredAt.Add(time.Second), Payload: []byte("tail")}
	d := model.Delivery{Topic: "access", Offset: 1}
	result, err := s.Heartbeat(ctx, h, d)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Missing)
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_streams WHERE complete_until=last_event_at AND last_heartbeat_at IS NOT NULL"))
	result, err = s.Heartbeat(ctx, h, d)
	require.NoError(t, err)
	require.True(t, result.Duplicate)
	d.Offset = 2
	result, err = s.Heartbeat(ctx, h, d)
	require.NoError(t, err)
	require.True(t, result.Duplicate)
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
	h.At = h.At.Add(time.Second)
	h.Payload = []byte("caught-up")
	d.Offset = 3
	_, err = s.Heartbeat(ctx, h, d)
	require.NoError(t, err)
	c, err := history.NewRepository(client).Coverage(ctx, e.OccurredAt.Add(-time.Hour))
	require.NoError(t, err)
	require.True(t, c.Known)
	require.Equal(t, e.OccurredAt, c.Watermark)
	require.Len(t, c.Gaps, 1)
	// Reads at/after W must never enter the features, even in the same minute.
	reads, err := history.NewRepository(client).ReadsSince(ctx, e.OccurredAt.Add(-time.Minute), e.OccurredAt)
	require.NoError(t, err)
	require.Empty(t, reads)
	reads, err = history.NewRepository(client).ReadsSince(ctx, e.OccurredAt.Add(-time.Minute), h.At)
	require.NoError(t, err)
	require.Len(t, reads, 1)
	require.Equal(t, int64(1), reads[0].Reads)
	require.NoError(t, s.CheckLostStreams(ctx, h.At.Add(30*time.Second), 30*time.Second))
	require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_streams WHERE lost_at IS NOT NULL"))
	lost := h.At.Add(31 * time.Second)
	require.NoError(t, s.CheckLostStreams(ctx, lost, 30*time.Second))
	require.NoError(t, s.CheckLostStreams(ctx, lost.Add(time.Second), 30*time.Second))
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE last_sequence IS NULL"))
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_streams WHERE closed_at=lost_at AND lost_at IS NOT NULL"))
	c, err = history.NewRepository(client).Coverage(ctx, e.OccurredAt.Add(-time.Hour))
	require.NoError(t, err)
	require.Equal(t, int64(1), c.LostStreams)
	require.Equal(t, e.OccurredAt, c.Watermark)
}
func TestClosingHeartbeatAndNewOpenStreamCoverage(t *testing.T) {
	client, s := database(t)
	ctx := context.Background()
	at := accessEvent(1).OccurredAt
	h := model.StreamHeartbeat{GatewayID: "gw", StreamID: "closed", At: at, Closing: true, Payload: []byte("closing")}
	d := model.Delivery{Topic: "read", Offset: 0}
	_, err := s.Heartbeat(ctx, h, d)
	require.NoError(t, err)
	_, err = s.Heartbeat(ctx, h, d)
	require.NoError(t, err)
	require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
	h.StreamID = "open"
	h.At = at.Add(time.Second)
	h.Closing = false
	h.Payload = []byte("open")
	d.Offset = 1
	_, err = s.Heartbeat(ctx, h, d)
	require.NoError(t, err)
	c, err := history.NewRepository(client).Coverage(ctx, at.Add(-time.Hour))
	require.NoError(t, err)
	require.True(t, c.Known)
	require.Equal(t, h.At, c.Watermark)
	require.Equal(t, int64(1), c.OpenStreams)
	require.Equal(t, int64(1), c.ClosedStreams)
}

func TestHeartbeatRollsBackProgressAndGapOnReceiptFailure(t *testing.T) {
	client, s := database(t)
	ctx := context.Background()
	e := accessEvent(1)
	deliver(t, s, e, 0)
	_, err := client.DB().ExecContext(ctx, db.Query{Name: "test:fail_receipt", QueryRaw: `ALTER TABLE ingestion_heartbeats ADD CONSTRAINT test_fail CHECK(false) NOT VALID`})
	require.NoError(t, err)
	h := model.StreamHeartbeat{GatewayID: e.GatewayID, StreamID: e.StreamID, Sequence: 3, At: e.OccurredAt.Add(time.Second), Payload: []byte("tail")}
	_, err = s.Heartbeat(ctx, h, model.Delivery{Topic: "access", Offset: 1})
	require.Error(t, err)
	require.NotErrorIs(t, err, model.ErrRejected)
	require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
	require.Equal(t, int64(1), scalar(t, client, "SELECT last_sequence FROM ingestion_streams"))
	require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_streams WHERE last_heartbeat_at IS NOT NULL"))
}

func TestStreamWithoutAnyHeartbeatExpiresFromRegistration(t *testing.T) {
	client, s := database(t)
	ctx := context.Background()
	e := accessEvent(1)
	deliver(t, s, e, 0)
	var registered time.Time
	require.NoError(t, client.DB().QueryRowContext(ctx, db.Query{Name: "test:registered", QueryRaw: `SELECT created_at FROM ingestion_streams`}).Scan(&registered))
	deliver(t, s, accessEvent(2), 1) // event traffic must not refresh the heartbeat deadline
	require.NoError(t, s.CheckLostStreams(ctx, registered.Add(30*time.Second), 30*time.Second))
	require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
	require.NoError(t, s.CheckLostStreams(ctx, registered.Add(30*time.Second+time.Microsecond), 30*time.Second))
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE last_sequence IS NULL"))
	require.True(t, deliver(t, s, accessEvent(2), 1).Duplicate)
	result, err := ingest(s, accessEvent(3), model.Delivery{Topic: "access", Offset: 2})
	require.NoError(t, err)
	require.True(t, result.Resumed)
	require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
}
