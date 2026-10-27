package dataset

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
)

const Horizon = time.Hour

type Label struct {
	Reads       *int64            `json:"reads"`
	HasRead     *bool             `json:"has_read"`
	AvailableAt time.Time         `json:"available_at"`
	Quality     model.DataQuality `json:"quality"`
}

type Row struct {
	Snapshot model.FeatureSnapshot `json:"snapshot"`
	Forecast model.DemandForecast  `json:"forecast"`
	Split    string                `json:"split"`
	Label    Label                 `json:"label"`
	Eligible bool                  `json:"eligible"`
}

// Rows freezes features at T, then attaches labels in a separate pass. A label
// is never supplied to the feature builder or predictor.
func Rows(ctx context.Context, req model.FeatureRequest, labels model.Coverage, start time.Time) ([]Row, error) {
	return rowsWithPeriods(ctx, req, labels, DefaultPeriods(start))
}

func rowsWithPeriods(ctx context.Context, req model.FeatureRequest, labels model.Coverage, periods Periods) ([]Row, error) {
	if err := features.ValidateCoverage(labels); err != nil {
		return nil, err
	}
	snapshots, err := features.NewService().Build(ctx, req)
	if err != nil {
		return nil, err
	}
	end := req.At.Add(Horizon)
	counts := make(map[model.VersionRef]int64)
	for _, e := range req.Events {
		if e.IsDemand() && !e.OccurredAt.Before(req.At) && e.OccurredAt.Before(end) {
			counts[model.VersionRef{Bucket: e.Bucket, ObjectKey: e.ObjectKey, VersionID: e.VersionID}]++
		}
	}
	result := make([]Row, 0, len(snapshots))
	for _, s := range snapshots {
		forecast, err := ewma.Predict(s, Horizon)
		if err != nil {
			return nil, err
		}
		split := periods.Split(req.At)
		q := features.Quality(labels, req.At, end)
		label := Label{AvailableAt: end, Quality: q}
		if q.Usable {
			n := counts[s.VersionRef]
			has := n > 0
			label.Reads = &n
			label.HasRead = &has
		}
		result = append(result, Row{Snapshot: s, Forecast: forecast, Split: split, Label: label, Eligible: split != "purged" && q.Usable && s.Quality.Usable})
	}
	return result, nil
}

func Split(at, start time.Time) string {
	return DefaultPeriods(start).Split(at)
}
