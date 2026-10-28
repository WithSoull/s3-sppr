package history

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

type repo struct{ db db.Client }

func NewRepository(client db.Client) repository.HistoryRepository { return &repo{db: client} }

func (r *repo) ReadsSince(ctx context.Context, from, until time.Time) ([]model.MinuteReads, error) {
	var rows []struct {
		Bucket    string    `db:"bucket"`
		ObjectKey string    `db:"object_key"`
		VersionID string    `db:"version_id"`
		Minute    time.Time `db:"minute"`
		Reads     int64     `db:"reads"`
		Bytes     int64     `db:"bytes"`
	}
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "history:reads_since", QueryRaw: `
		SELECT bucket, object_key, version_id, date_trunc('minute',occurred_at) AS minute,
 count(*) AS reads, sum(bytes)::bigint AS bytes FROM access_events
 WHERE occurred_at >= $1 AND occurred_at < $2 AND operation='read' AND origin='client' AND result='success'
 GROUP BY bucket,object_key,version_id,date_trunc('minute',occurred_at)`}, from, until); err != nil {
		return nil, err
	}
	out := make([]model.MinuteReads, 0, len(rows))
	for _, row := range rows {
		out = append(out, model.MinuteReads{
			VersionRef: model.VersionRef{Bucket: row.Bucket, ObjectKey: row.ObjectKey, VersionID: row.VersionID},
			Minute:     row.Minute.UTC(), Reads: row.Reads, Bytes: row.Bytes,
		})
	}
	return out, nil
}

// Coverage implements the common heartbeat watermark across all topic streams.
func (r *repo) Coverage(ctx context.Context, since time.Time) (model.Coverage, error) {

	var streams []streamProgress
	if err := r.db.DB().ScanAllContext(ctx, &streams, db.Query{Name: "history:coverage", QueryRaw: `SELECT first_event_at,first_heartbeat_at,last_heartbeat_at,complete_until,closed_at,lost_at,created_at FROM ingestion_streams`}); err != nil {
		return model.Coverage{}, err
	}
	c := coverageOf(streams)
	var err error
	err = r.db.DB().QueryRowContext(ctx, db.Query{Name: "history:gaps24h", QueryRaw: `SELECT count(*) FROM ingestion_gaps WHERE before_time > now()-interval '24 hours'`}).Scan(&c.Gaps24h)
	if err != nil {
		return c, err
	}
	var gaps []struct {
		After  *time.Time `db:"after_time"`
		Before time.Time  `db:"before_time"`
	}
	err = r.db.DB().ScanAllContext(ctx, &gaps, db.Query{Name: "history:gaps", QueryRaw: `SELECT after_time,before_time FROM ingestion_gaps WHERE before_time >= $1`}, since)
	if err != nil {
		return c, err
	}
	for _, g := range gaps {
		gapStart := c.Start
		if gapStart.IsZero() || since.Before(gapStart) {
			gapStart = since
		}
		if g.After != nil {
			gapStart = g.After.UTC()
		}
		if !gapStart.IsZero() && g.Before.After(gapStart) {
			c.Gaps = append(c.Gaps, model.Interval{Start: gapStart, End: g.Before.UTC()})
		}
	}
	return c, nil
}

type streamProgress struct {
	FirstHeartbeat *time.Time `db:"first_heartbeat_at"`
	First          *time.Time `db:"first_event_at"`
	Heartbeat      *time.Time `db:"last_heartbeat_at"`
	Complete       *time.Time `db:"complete_until"`
	Closed         *time.Time `db:"closed_at"`
	Lost           *time.Time `db:"lost_at"`
	Created        time.Time  `db:"created_at"`
}

func coverageOf(streams []streamProgress) model.Coverage {
	c := model.Coverage{}
	var openMin, closedMax, lastClosed time.Time
	unknownOpen := false
	for _, s := range streams {
		start := s.Created
		if s.First != nil {
			start = *s.First
		}
		if s.FirstHeartbeat != nil && (s.First == nil || s.FirstHeartbeat.Before(start)) {
			start = *s.FirstHeartbeat
		}
		if start.IsZero() && s.Heartbeat != nil {
			start = *s.Heartbeat
		}

		if c.Start.IsZero() || start.Before(c.Start) {
			c.Start = start.UTC()
		}
		if s.Lost != nil {
			c.LostStreams++
		}
		if s.Closed == nil {
			c.OpenStreams++
			if s.Complete == nil || s.Heartbeat == nil {
				unknownOpen = true
				continue
			}
			if openMin.IsZero() || s.Complete.Before(openMin) {
				openMin = s.Complete.UTC()
			}
		} else {
			c.ClosedStreams++
			if s.Closed.After(lastClosed) {
				lastClosed = s.Closed.UTC()
			}
			if s.Complete != nil && s.Complete.After(closedMax) {
				closedMax = s.Complete.UTC()
			}
		}
	}
	if c.OpenStreams > 0 {
		if !unknownOpen {
			c.Watermark = openMin
		}
	} else if !closedMax.IsZero() {
		c.Watermark = closedMax
		if lastClosed.Before(c.Watermark) {
			c.Watermark = lastClosed
		}
	}
	c.Known = !c.Watermark.IsZero() && !c.Start.IsZero() && !c.Watermark.Before(c.Start)
	return c
}
