package dataset

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
)

type HistoryOptions struct {
	Now             time.Time
	Window, Horizon time.Duration
}

// FromHistory writes bounded snapshot batches. Coverage is read BEFORE aggregates:
// a later commit may add rows, but cannot make labels ahead of this W eligible.
// The caller creates a fresh output directory. No test rows are produced.
func FromHistory(ctx context.Context, history repository.HistoryRepository, catalog []model.Version, dir string, o HistoryOptions) (Manifest, error) {
	var m Manifest
	if o.Horizon <= 0 || o.Horizon%time.Minute != 0 || o.Window < 7*o.Horizon {
		return m, fmt.Errorf("history dataset needs whole-minute horizon and window >= 7 horizons")
	}
	end := o.Now.UTC().Truncate(time.Minute)
	coverage, err := history.Coverage(ctx, end.Add(-o.Window-features.MaxWindow))
	if err != nil {
		return m, err
	}
	if !coverage.Known {
		return m, fmt.Errorf("unknown_completeness")
	}
	if coverage.Watermark.Before(end) {
		end = coverage.Watermark.UTC().Truncate(time.Minute)
	}
	start := end.Add(-o.Window).Truncate(time.Minute)
	boundary := end.Add(-o.Window / 7).Truncate(time.Minute)
	if !boundary.After(start) {
		return m, fmt.Errorf("empty train interval")
	}
	reads, err := history.ReadsSince(ctx, start.Add(-features.MaxWindow), end)
	if err != nil {
		return m, err
	}
	m = Manifest{GeneratorVersion: "history-v1", FeatureRulesVersion: model.FeatureRulesVersion, HorizonSeconds: int64(o.Horizon / time.Second),
		Train: model.Interval{Start: start, End: boundary}, Validation: model.Interval{Start: boundary, End: end}, Test: model.Interval{Start: end, End: end},
		Format: "csv.gz", SnapshotSeconds: 900, TrainEverySeconds: 900, CompactColumns: CompactColumns(),
		CoveragePolicy: "history repository Coverage; complete features and [t,t+H) labels only; no assumed watermark",
		Provenance:     map[string]string{"catalog_limit": "Metadata ListPlacements omits versions deleted during the window; overwritten COLD versions may also be absent", "source": "live minute aggregates"},
		Rows:           map[string]int64{}, Eligible: map[string]int64{}, SHA256: map[string]string{}}
	writer, err := openRows(dir, "csv.gz")
	if err != nil {
		return m, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
		}
	}()
	builder := features.NewService()
	for at := start; !at.Add(o.Horizon).After(end); at = at.Add(15 * time.Minute) {
		if err := ctx.Err(); err != nil {
			return m, err
		}
		labelEnd := at.Add(o.Horizon)
		if !features.Quality(coverage, at, labelEnd).Usable {
			continue
		}
		split := "train"
		if !at.Before(boundary) {
			split = "validation"
		} else if labelEnd.After(boundary) {
			continue
		}
		featureReads := make([]model.MinuteReads, 0)
		for _, read := range reads {
			if !read.Minute.Before(at.Add(-features.MaxWindow)) && read.Minute.Before(at) {
				featureReads = append(featureReads, read)
			}
		}
		snapshots, err := builder.Build(ctx, model.FeatureRequest{At: at, Catalog: catalog, Reads: featureReads, Coverage: coverage})
		if err != nil {
			return m, err
		}
		counts := map[model.VersionRef]int64{}
		for _, read := range reads {
			if !read.Minute.Before(at) && read.Minute.Before(labelEnd) {
				counts[read.VersionRef] += read.Reads
			}
		}
		for _, s := range snapshots {
			if !s.Quality.Usable {
				continue
			}
			n := counts[s.VersionRef]
			has := n > 0
			f, err := ewma.Predict(s, o.Horizon)
			if err != nil {
				return m, err
			}
			row := Row{Snapshot: s, Forecast: f, Split: split, Eligible: true, Label: Label{Reads: &n, HasRead: &has, AvailableAt: labelEnd, Quality: model.DataQuality{Usable: true, Reason: "complete"}}}
			if err := writer.Write(row); err != nil {
				return m, err
			}
			m.Rows[split]++
			m.Eligible[split]++
			m.LastLabelAvailableAt = &labelEnd
		}
	}
	if err := writer.Close(); err != nil {
		return m, err
	}
	closed = true
	for _, name := range writer.Names() {
		hash, err := HashFile(filepath.Join(dir, name))
		if err != nil {
			return m, err
		}
		m.SHA256[name] = hash
	}
	if m.Rows["train"] == 0 || m.Rows["validation"] == 0 {
		return m, fmt.Errorf("empty eligible train or validation split")
	}
	return m, writeJSON(filepath.Join(dir, "manifest.json"), m)
}
