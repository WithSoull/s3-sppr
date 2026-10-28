package planner

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
)

func (s *plannerService) GetCommand(ctx context.Context, commandID string) (model.Command, error) {
	return s.plans.GetCommand(ctx, commandID)
}

func (s *plannerService) ListCommands(ctx context.Context, f model.CommandFilter) (model.CommandPage, error) {
	f.PageSize = pageSize(f.PageSize)
	commands, err := s.plans.ListCommands(ctx, f)
	if err != nil {
		return model.CommandPage{}, err
	}
	page := model.CommandPage{Commands: commands}
	if len(commands) == f.PageSize {
		last := commands[len(commands)-1]
		page.NextPageToken = last.CreatedAt.Format(time.RFC3339Nano) + "|" + last.CommandID
	}
	return page, nil
}

func (s *plannerService) GetPlan(ctx context.Context, planID string) (model.Plan, error) {
	return s.plans.GetPlan(ctx, planID, false)
}

func (s *plannerService) LatestPlan(ctx context.Context) (*model.Plan, error) {
	return s.plans.LatestPlan(ctx)
}

func (s *plannerService) ListPlans(ctx context.Context, f model.PlanFilter) (model.PlanPage, error) {
	f.PageSize = pageSize(f.PageSize)
	plans, err := s.plans.ListPlans(ctx, f)
	if err != nil {
		return model.PlanPage{}, err
	}
	page := model.PlanPage{Plans: plans}
	if len(plans) == f.PageSize {
		page.NextPageToken = strconv.FormatInt(plans[len(plans)-1].Revision, 10)
	}
	return page, nil
}

func (s *plannerService) ListRecommendations(ctx context.Context, f model.RecommendationFilter) (model.RecommendationPage, error) {
	if _, err := s.plans.GetPlan(ctx, f.PlanID, false); err != nil {
		return model.RecommendationPage{}, err
	}
	f.PageSize = pageSize(f.PageSize)
	recs, err := s.plans.ListRecommendations(ctx, f)
	if err != nil {
		return model.RecommendationPage{}, err
	}
	page := model.RecommendationPage{Recommendations: recs}
	if len(recs) == f.PageSize {
		page.NextPageToken = strconv.Itoa(recs[len(recs)-1].Order)
	}
	return page, nil
}

func (s *plannerService) GetSettings(ctx context.Context) (model.PlannerSettings, error) {
	return s.plans.GetSettings(ctx)
}

func (s *plannerService) Status(ctx context.Context) (model.PlannerStatus, error) {
	st := model.PlannerStatus{MethodVersion: s.predictor.MethodVersion(), ModelVersion: s.predictor.ModelVersion(), FallbackReason: s.cfg.FallbackReason}
	usage, err := s.placements.ListStorageClasses(ctx)
	if err != nil {
		return st, fmt.Errorf("read storage classes: %w", err)
	}
	for _, u := range usage {
		state := model.ClassState{CapacityBytes: u.CapacityBytes, MaxUtilization: u.MaxUtilization, UsedBytes: u.PlacedBytes + u.CopyBytes}
		if u.Class == model.StorageClassHot {
			st.Hot = state
		} else if u.Class == model.StorageClassCold {
			st.Cold = state
		}
	}
	at := s.now().UTC()
	coverage, err := s.history.Coverage(ctx, at.Add(-features.MaxWindow))
	if err != nil {
		return st, fmt.Errorf("read history coverage: %w", err)
	}
	st.History = coverage
	st.HistoryQuality = model.DataQuality{Reason: "unknown_completeness"}
	if coverage.Known {
		st.HistoryLagSeconds = max(0, at.Sub(coverage.Watermark).Seconds())
		st.HistoryQuality = features.Quality(coverage, maxTime(coverage.Start, coverage.Watermark.Add(-features.MaxWindow)), coverage.Watermark)
		if at.Sub(coverage.Watermark) > s.cfg.MaxHistoryLag {
			st.HistoryQuality = model.DataQuality{Reason: "история отстаёт"}
		}
	}
	catalog, err := s.placements.ListCatalog(ctx)
	if err != nil {
		return st, err
	}
	snapshots := map[string]model.FeatureSnapshot{}
	if coverage.Known {
		historyAt := coverage.Watermark
		coverage, err = s.history.Coverage(ctx, historyAt.Add(-features.MaxWindow))
		if err != nil {
			return st, err
		}
		var featureQuality model.DataQuality
		snapshots, featureQuality, err = s.forecastFeatures(ctx, historyAt, catalog, coverage)
		if err != nil {
			return st, err
		}
		st.History.Gaps = coverage.Gaps
		if st.HistoryQuality.Usable && !featureQuality.Usable {
			st.HistoryQuality = featureQuality
		}
	}
	for _, p := range catalog {
		if !p.IsCurrent {
			continue
		}
		q := st.HistoryQuality
		reads := int64(0)
		if fs, ok := snapshots[p.VersionID]; ok {
			q = fs.Quality
			reads = fs.Features.Reads24h
		} else if coverage.Known {
			q = model.DataQuality{Reason: "unobserved"}
		}
		if st.HistoryLagSeconds > s.cfg.MaxHistoryLag.Seconds() {
			q = model.DataQuality{Reason: "история отстаёт"}
		}
		state := "есть чтения"
		if !q.Usable {
			state = "история неполна"
		} else if reads == 0 {
			state = "чтений нет"
		}
		st.Objects = append(st.Objects, model.ObjectHistory{VersionRef: model.VersionRef{Bucket: p.Bucket, ObjectKey: p.Key, VersionID: p.VersionID}, Quality: q, State: state, Reads24h: reads})
	}
	if st.Migration, err = s.migrations.GetSettings(ctx); err != nil {
		return st, err
	}
	if st.Budget, err = s.migrations.GetBudget(ctx); err != nil {
		return st, err
	}
	if st.LatestPlan, err = s.plans.LatestPlan(ctx); err != nil {
		return st, err
	}
	active, err := s.actions.ListActive(ctx)
	if err != nil {
		return st, err
	}
	for _, a := range active {
		if a.Status == model.ActionPending {
			st.PendingActions++
		} else {
			st.RunningActions++
		}
	}
	return st, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
