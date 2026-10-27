package dataset_test

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func TestRowsLesson(t *testing.T) {
	req := dataset.Lesson()
	coverage := req.Coverage
	coverage.Watermark = req.At.Add(time.Hour)
	rows, err := dataset.Rows(context.Background(), req, coverage, req.Coverage.Start)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	for i, want := range []int64{2, 0, 1} {
		require.Equal(t, want, *rows[i].Label.Reads)
		require.Equal(t, want > 0, *rows[i].Label.HasRead)
		require.True(t, rows[i].Eligible)
		require.Equal(t, "train", rows[i].Split)
	}
	// The new c version is deleted within the horizon, but its saved row remains.
	require.Equal(t, int64(100), rows[2].Snapshot.SizeBytes)
}

func TestRowsUnavailableLabels(t *testing.T) {
	req := dataset.Lesson()
	cases := []struct {
		name     string
		coverage model.Coverage
		reason   string
	}{
		{"unknown", model.Coverage{}, "unknown_completeness"},
		{"pending", req.Coverage, "watermark_behind"},
		{"gap", model.Coverage{Known: true, Start: req.Coverage.Start, Watermark: req.At.Add(time.Hour), Gaps: []model.Interval{{Start: req.At.Add(time.Minute), End: req.At.Add(2 * time.Minute)}}}, "gap"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := dataset.Rows(context.Background(), req, tt.coverage, req.Coverage.Start)
			require.NoError(t, err)
			for _, r := range rows {
				require.Nil(t, r.Label.Reads)
				require.Nil(t, r.Label.HasRead)
				require.False(t, r.Eligible)
				require.Equal(t, tt.reason, r.Label.Quality.Reason)
			}
		})
	}
}

func TestSplit(t *testing.T) {
	start := dataset.Lesson().At
	cases := []struct {
		hours float64
		want  string
	}{{0, "train"}, {47, "train"}, {47.5, "purged"}, {48, "validation"}, {71, "validation"}, {71.5, "purged"}, {72, "test"}, {167, "test"}, {167.5, "purged"}, {168, "purged"}, {-1, "purged"}}
	for _, tt := range cases {
		at := start.Add(time.Duration(tt.hours * float64(time.Hour)))
		require.Equal(t, tt.want, dataset.Split(at, start), at.String())
	}
}
