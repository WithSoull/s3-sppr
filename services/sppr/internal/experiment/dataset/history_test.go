package dataset

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestFromHistory(t *testing.T) {
	for _, tt := range []struct {
		name string
		gap  bool
	}{{"coverage and planner feature parity", false}, {"gap removes affected slices", true}} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			history := mocks.NewHistoryRepositoryMock(mc)
			now := time.Date(2026, 10, 28, 12, 0, 45, 0, time.UTC)
			end := now.Truncate(time.Minute)
			start := end.Add(-7 * time.Hour)
			c := model.Coverage{Known: true, Start: start.Add(-48 * time.Hour), Watermark: end.Add(-30*time.Minute + 30*time.Second)}
			if tt.gap {
				c.Gaps = []model.Interval{{Start: end.Add(-time.Hour), End: end.Add(-59 * time.Minute)}}
			}
			ref := model.VersionRef{Bucket: "b", ObjectKey: "k", VersionID: "v"}
			catalog := []model.Version{{VersionRef: ref, SizeBytes: 10, CreatedAt: c.Start}}
			reads := []model.MinuteReads{{VersionRef: ref, Minute: end.Add(-2 * time.Hour), Reads: 3, Bytes: 30}, {VersionRef: ref, Minute: end.Add(-29 * time.Minute), Reads: 99, Bytes: 990}}
			history.CoverageMock.Return(c, nil)
			history.ReadsSinceMock.Return(reads, nil)
			dir := t.TempDir()
			m, err := FromHistory(context.Background(), history, catalog, dir, HistoryOptions{Now: now, Window: 7 * time.Hour, Horizon: 2 * time.Minute})

			require.NoError(t, err)
			require.EqualValues(t, 120, m.HorizonSeconds)
			require.Equal(t, m.Test.Start, m.Test.End)
			require.Equal(t, end.Add(-30*time.Minute), m.Validation.End)
			require.Contains(t, m.Provenance["catalog_limit"], "deleted")
			count := 0
			for _, split := range []string{"train", "validation"} {
				f, err := os.Open(filepath.Join(dir, split+".csv.gz"))
				require.NoError(t, err)
				gz, err := gzip.NewReader(f)
				require.NoError(t, err)
				all, err := csv.NewReader(gz).ReadAll()
				require.NoError(t, err)
				require.NoError(t, gz.Close())
				require.NoError(t, f.Close())
				for _, values := range all[1:] {
					count++
					at, err := time.Parse(time.RFC3339Nano, values[3])
					require.NoError(t, err)
					require.Equal(t, at.Truncate(time.Minute), at)
					require.False(t, at.Add(2*time.Minute).After(c.Watermark))
					if tt.gap {
						require.False(t, at.Add(2*time.Minute).After(c.Gaps[0].Start))
					}
					filtered := []model.MinuteReads{}
					for _, read := range reads {
						if !read.Minute.Before(at.Add(-24*time.Hour)) && read.Minute.Before(at) {
							filtered = append(filtered, read)
						}
					}
					snapshots, err := features.NewService().Build(context.Background(), model.FeatureRequest{At: at, Catalog: catalog, Reads: filtered, Coverage: c})
					require.NoError(t, err)
					expected := compactValues(Row{Snapshot: snapshots[0]})
					require.Equal(t, expected[:23], values[:23])
					cols := map[string]string{}
					for i, k := range CompactColumns() {
						cols[k] = values[i]
					}
					require.NotEqual(t, "99", cols["label_reads"])
				}
			}
			require.Greater(t, count, 0)
		})
	}
}
