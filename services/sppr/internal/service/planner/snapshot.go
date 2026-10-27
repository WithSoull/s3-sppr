package planner

import (
	"context"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
)

// snapshot is everything one planning run needs, read at one moment.
type snapshot struct {
	input   model.PlanInput
	quality model.DataQuality
}

func paramsOf(s model.PlannerSettings, maxRejected int) model.PlannerParams {
	return model.PlannerParams{
		Horizon:    s.Horizon,
		HotFixedMs: s.HotFixedMs, HotMiBPerMs: s.HotMiBPerMs,
		ColdFixedMs: s.ColdFixedMs, ColdMiBPerMs: s.ColdMiBPerMs,
		PenaltyMsPerMiB: s.PenaltyMsPerMiB, MinGainMs: s.MinGainMs,
		HotUtilization: s.HotUtilization, ColdUtilization: s.ColdUtilization,
		MaxRejectedSaved: maxRejected,
	}
}

func (s *plannerService) snapshot(ctx context.Context, at time.Time, params model.PlannerParams) (snapshot, error) {
	catalog, err := s.placements.ListCatalog(ctx)
	if err != nil {
		return snapshot{}, fmt.Errorf("read catalogue: %w", err)
	}
	usage, err := s.placements.ListStorageClasses(ctx)
	if err != nil {
		return snapshot{}, fmt.Errorf("read storage classes: %w", err)
	}
	settings, err := s.migrations.GetSettings(ctx)
	if err != nil {
		return snapshot{}, fmt.Errorf("read migration settings: %w", err)
	}
	budget, err := s.migrations.GetBudget(ctx)
	if err != nil {
		return snapshot{}, fmt.Errorf("read budget: %w", err)
	}
	pinList, err := s.migrations.ListPins(ctx)
	if err != nil {
		return snapshot{}, fmt.Errorf("read pins: %w", err)
	}
	active, err := s.actions.ListActive(ctx)
	if err != nil {
		return snapshot{}, fmt.Errorf("read active actions: %w", err)
	}

	in := model.PlanInput{At: at, Params: params, MethodVersion: s.predictor.MethodVersion()}
	for _, u := range usage {
		state := model.ClassState{CapacityBytes: u.CapacityBytes, MaxUtilization: u.MaxUtilization, UsedBytes: u.PlacedBytes + u.CopyBytes}
		switch u.Class {
		case model.StorageClassHot:
			in.Hot = state
		case model.StorageClassCold:
			in.Cold = state
		}
	}
	// Accepted actions will spend budget and take target space that Metadata
	// does not count until they reserve their copy.
	busy := make(map[string]bool, len(active))
	remaining := budget.Remaining
	for _, a := range active {
		busy[a.VersionID] = true
		if a.Status != model.ActionPending && a.Step != model.StepReserving {
			continue
		}
		if a.Status == model.ActionPending {
			remaining -= a.SizeBytes
		}
		if a.TargetClass == model.StorageClassHot {
			in.Hot.UsedBytes += a.SizeBytes
		} else {
			in.Cold.UsedBytes += a.SizeBytes
		}
	}
	in.BudgetBytes = max(remaining, 0)
	pinned := make(map[[2]string]bool, len(pinList))
	for _, p := range pinList {
		pinned[[2]string{p.Bucket, p.Key}] = true
	}

	snapshots, quality, err := s.forecastFeatures(ctx, at, catalog)
	if err != nil {
		return snapshot{}, err
	}
	for _, p := range catalog {
		it := model.PlanItem{Placement: p, ReadFraction: 1}
		switch {
		case pinned[[2]string{p.Bucket, p.Key}]:
			it.Blocker = model.BlockerPinned
		case p.MigrationInProgress || busy[p.VersionID]:
			it.Blocker = model.BlockerInProgress
		// The executor applies the same rule; generation 1 was chosen at write time.
		case p.Generation > 1 && at.Sub(p.UpdatedAt) < settings.MinInterval:
			it.Blocker = model.BlockerMinInterval
		}
		if fs, ok := snapshots[p.VersionID]; ok && p.IsCurrent {
			f := fs.Features
			it.Features = &f
			if f.Reads24h > 0 {
				it.ReadFraction = f.MeanReadFraction
			}
			if forecast, err := s.predictor.Predict(fs, params.Horizon); err == nil {
				it.Forecast = &forecast
			}
		}
		in.Items = append(in.Items, it)
	}
	return snapshot{input: in, quality: quality}, nil
}

// forecastFeatures builds feature snapshots of the catalogue from the last day
// of minute aggregates and reports whether the history behind them is usable.
func (s *plannerService) forecastFeatures(ctx context.Context, at time.Time, catalog []model.Placement) (map[string]model.FeatureSnapshot, model.DataQuality, error) {
	since := at.Add(-features.MaxWindow)
	reads, err := s.history.ReadsSince(ctx, since)
	if err != nil {
		return nil, model.DataQuality{}, fmt.Errorf("read demand history: %w", err)
	}
	coverage, err := s.history.Coverage(ctx, since)
	if err != nil {
		return nil, model.DataQuality{}, fmt.Errorf("read history coverage: %w", err)
	}
	versions := make([]model.Version, 0, len(catalog))
	for _, p := range catalog {
		versions = append(versions, model.Version{
			VersionRef: model.VersionRef{Bucket: p.Bucket, ObjectKey: p.Key, VersionID: p.VersionID},
			SizeBytes:  p.SizeBytes, CreatedAt: p.CreatedAt,
		})
	}
	coverage = assumeCaughtUp(coverage, at, versions)
	rows, err := s.features.Build(ctx, model.FeatureRequest{At: at, Catalog: versions, Reads: reads, Coverage: coverage})
	if err != nil {
		return nil, model.DataQuality{}, fmt.Errorf("build features: %w", err)
	}
	quality := model.DataQuality{Usable: coverage.Known, Reason: "complete"}
	if !coverage.Known {
		quality.Reason = "unknown_completeness"
	}
	out := make(map[string]model.FeatureSnapshot, len(rows))
	for _, r := range rows {
		out[r.VersionID] = r
		if !r.Quality.Usable && quality.Usable {
			quality = r.Quality
		}
	}
	return out, quality, nil
}

// assumeCaughtUp is the completeness rule until heartbeats exist (stage 5):
// the Ingestor is taken to have every event up to now, and history to start no
// later than the oldest catalogue version, because Gateway publishes the write
// of a version only after Metadata created it. Gaps the Ingestor detected
// still make history unusable.
func assumeCaughtUp(c model.Coverage, at time.Time, versions []model.Version) model.Coverage {
	if !c.Known {
		return c
	}
	c.Watermark = at
	for _, v := range versions {
		if v.CreatedAt.Before(c.Start) {
			c.Start = v.CreatedAt
		}
	}
	return c
}
