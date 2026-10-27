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

func (r *repo) ReadsSince(ctx context.Context, from time.Time) ([]model.MinuteReads, error) {
	var rows []struct {
		Bucket    string    `db:"bucket"`
		ObjectKey string    `db:"object_key"`
		VersionID string    `db:"version_id"`
		Minute    time.Time `db:"minute"`
		Reads     int64     `db:"reads"`
		Bytes     int64     `db:"bytes"`
	}
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "history:reads_since", QueryRaw: `
		SELECT bucket, object_key, version_id, minute, reads, bytes FROM read_aggregates
		WHERE minute >= date_trunc('minute', $1::timestamptz)`}, from); err != nil {
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

// Coverage reports the streams' first event and the oldest last event as the
// watermark; a stream with no recent events holds it back. Gaps without a
// known start begin at the coverage start.
func (r *repo) Coverage(ctx context.Context, since time.Time) (model.Coverage, error) {
	var streams int64
	var start, watermark *time.Time
	if err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "history:coverage", QueryRaw: `
		SELECT count(*), min(first_event_at), min(last_event_at) FROM ingestion_streams`}).
		Scan(&streams, &start, &watermark); err != nil {
		return model.Coverage{}, err
	}
	if streams == 0 || start == nil || watermark == nil {
		return model.Coverage{}, nil
	}
	c := model.Coverage{Known: true, Start: start.UTC(), Watermark: watermark.UTC()}
	var gaps []struct {
		After  *time.Time `db:"after_time"`
		Before time.Time  `db:"before_time"`
	}
	if err := r.db.DB().ScanAllContext(ctx, &gaps, db.Query{Name: "history:gaps", QueryRaw: `
		SELECT after_time, before_time FROM ingestion_gaps WHERE before_time > $1`}, since); err != nil {
		return model.Coverage{}, err
	}
	for _, g := range gaps {
		gapStart := c.Start
		if g.After != nil {
			gapStart = g.After.UTC()
		}
		if g.Before.After(gapStart) {
			c.Gaps = append(c.Gaps, model.Interval{Start: gapStart, End: g.Before.UTC()})
		}
	}
	return c, nil
}
