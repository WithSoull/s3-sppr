package features_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/stretchr/testify/require"
)

func TestBuildLesson(t *testing.T) {
	req := dataset.Lesson()
	rows, err := features.NewService().Build(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	a, b, c := rows[0], rows[1], rows[2]
	require.Equal(t, int64(2), a.Features.Reads1h)
	require.Equal(t, int64(0), a.Features.Reads5m)
	require.Equal(t, 1800.0, *a.Features.LastReadSeconds)
	require.Equal(t, 3600.0, *a.Features.PreviousReadSeconds)
	require.InDelta(t, 0.75, a.Features.MeanReadFraction, 1e-12)
	require.InDelta(t, math.Ln2*(0.5+math.Sqrt(0.5)), a.Features.EWMA1h, 1e-12)
	require.Equal(t, int64(3), a.Features.BucketReads1h)
	require.InDelta(t, 2.0/3, a.Features.BucketActiveFraction, 1e-12)
	require.Equal(t, 86400.0, a.Features.ObservedSeconds)
	require.True(t, a.Quality.Usable)
	require.Nil(t, b.Features.LastReadSeconds)
	require.Zero(t, b.Features.Reads24h)
	require.True(t, b.Quality.Usable)
	require.Equal(t, 900.0, c.Features.ObservedSeconds)
	require.Equal(t, "warmup", c.Quality.Reason)
}

func TestBuildTimeBoundariesAndFutureIsolation(t *testing.T) {
	req := dataset.Lesson()
	req.Events = nil
	base := dataset.Lesson().Events[0]
	cases := []struct {
		name                                        string
		age                                         time.Duration
		reads5m, reads1h, reads6h, reads24h, dayAgo int64
	}{
		{"at T belongs to label", 0, 0, 0, 0, 0, 0},
		{"future", -time.Second, 0, 0, 0, 0, 0},
		{"5m inclusive", 5 * time.Minute, 1, 1, 1, 1, 0},
		{"outside 5m", 5*time.Minute + time.Nanosecond, 0, 1, 1, 1, 0},
		{"1h inclusive", time.Hour, 0, 1, 1, 1, 0},
		{"6h inclusive", 6 * time.Hour, 0, 0, 1, 1, 0},
		{"24h inclusive", 24 * time.Hour, 0, 0, 0, 1, 1},
		{"daily right endpoint excluded", 23 * time.Hour, 0, 0, 0, 1, 0},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			e := base
			e.OccurredAt = req.At.Add(-tt.age)
			input := req
			input.Events = []model.AccessEvent{e}
			rows, err := features.NewService().Build(context.Background(), input)
			require.NoError(t, err)
			f := rows[0].Features
			require.Equal(t, tt.reads5m, f.Reads5m)
			require.Equal(t, tt.reads1h, f.Reads1h)
			require.Equal(t, tt.reads6h, f.Reads6h)
			require.Equal(t, tt.reads24h, f.Reads24h)
			require.Equal(t, tt.dayAgo, f.ReadsDayAgo)
		})
	}
	input := dataset.Lesson()
	want, err := features.NewService().Build(context.Background(), input)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		e := base
		e.OccurredAt = input.At.Add(time.Duration(i) * time.Second)
		e.Bytes = 99
		input.Events = append(input.Events, e)
	}
	got, err := features.NewService().Build(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBuildCompleteness(t *testing.T) {
	base := dataset.Lesson()
	at := base.At
	cases := []struct {
		name     string
		coverage model.Coverage
		want     model.DataQuality
	}{
		{"unknown", model.Coverage{}, model.DataQuality{Reason: "unknown_completeness"}},
		{"watermark behind", model.Coverage{Known: true, Start: at.Add(-24 * time.Hour), Watermark: at.Add(-time.Second)}, model.DataQuality{Reason: "watermark_behind"}},
		{"gap intersects past", model.Coverage{Known: true, Start: at.Add(-24 * time.Hour), Watermark: at, Gaps: []model.Interval{{Start: at.Add(-time.Hour), End: at.Add(-time.Minute)}}}, model.DataQuality{Reason: "gap"}},
		{"gap starts at T", model.Coverage{Known: true, Start: at.Add(-24 * time.Hour), Watermark: at, Gaps: []model.Interval{{Start: at, End: at.Add(time.Hour)}}}, model.DataQuality{Usable: true, Reason: "complete"}},
		{"collection just started", model.Coverage{Known: true, Start: at, Watermark: at}, model.DataQuality{Reason: "unobserved"}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			req := base
			req.Coverage = tt.coverage
			rows, err := features.NewService().Build(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, tt.want, rows[0].Quality)
			if tt.want.Reason == "gap" {
				require.False(t, rows[2].Quality.Usable, "a new version also uses bucket history")
			}
		})
	}
}

func TestBuildLifecycleAndOverwrites(t *testing.T) {
	req := dataset.Lesson()
	req.Events = nil
	dead := req.At.Add(-time.Hour)
	old := req.Catalog[0]
	old.VersionID = "a0"
	old.CreatedAt = req.At.Add(-48 * time.Hour)
	old.DeletedAt = &dead
	req.Catalog[0].CreatedAt = dead
	req.Catalog = append(req.Catalog, old)
	rows, err := features.NewService().Build(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, int64(1), rows[0].Features.Overwrites24h)
	// Later deletion must not remove a historical row or its size.
	deleted := req.At.Add(time.Minute)
	req.Catalog[0].DeletedAt = &deleted
	got, err := features.NewService().Build(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, rows, got)
}

func TestBuildValidation(t *testing.T) {
	cases := []struct {
		name   string
		change func(*model.FeatureRequest)
	}{
		{"missing time", func(r *model.FeatureRequest) { r.At = time.Time{} }},
		{"duplicate version", func(r *model.FeatureRequest) { r.Catalog = append(r.Catalog, r.Catalog[0]) }},
		{"negative size", func(r *model.FeatureRequest) { r.Catalog[0].SizeBytes = -1 }},
		{"read too large", func(r *model.FeatureRequest) { r.Events[0].Bytes = 101 }},
		{"invalid coverage", func(r *model.FeatureRequest) { r.Coverage.Start = r.At.Add(time.Hour) }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			req := dataset.Lesson()
			tt.change(&req)
			_, err := features.NewService().Build(context.Background(), req)
			require.Error(t, err)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := features.NewService().Build(ctx, dataset.Lesson())
	require.ErrorIs(t, err, context.Canceled)
}

func TestBuildDemandFiltering(t *testing.T) {
	req := dataset.Lesson()
	cases := []struct {
		name, operation, origin, result string
		want                            int64
	}{
		{"client read", model.OperationRead, model.OriginClient, model.ResultSuccess, 1},
		{"write", model.OperationWrite, model.OriginClient, model.ResultSuccess, 0},
		{"migration", model.OperationRead, model.OriginMigration, model.ResultSuccess, 0},
		{"failed read", model.OperationRead, model.OriginClient, "error", 0},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			e := req.Events[0]
			e.Operation = tt.operation
			e.Origin = tt.origin
			e.Result = tt.result
			input := req
			input.Events = []model.AccessEvent{e}
			rows, err := features.NewService().Build(context.Background(), input)
			require.NoError(t, err)
			require.Equal(t, tt.want, rows[0].Features.Reads1h)
		})
	}
}
