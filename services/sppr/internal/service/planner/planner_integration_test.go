//go:build integration

package planner_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	actionRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/action"
	pinRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/pin"
	planRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/plan"
	settingsRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/settings"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	migrationService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/migration"
	plannerService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/pg"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/transaction"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

type stand struct {
	tx         db.TxManager
	plans      repository.PlanRepository
	actions    repository.ActionRepository
	migrations service.MigrationService
	planner    service.PlannerService
}

// database applies every SPPR migration to a fresh schema of SPPR_TEST_PG_DSN.
func database(t *testing.T) stand {
	t.Helper()
	dsn := os.Getenv("SPPR_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set SPPR_TEST_PG_DSN to a test PostgreSQL database")
	}
	logger.SetNopLogger()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	schema := fmt.Sprintf("sppr_planner_test_%d", time.Now().UnixNano())
	_, err = conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		require.NoError(t, err)
	})
	poolDSN := dsn
	if strings.Contains(poolDSN, "://") {
		u, err := url.Parse(poolDSN)
		require.NoError(t, err)
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		poolDSN = u.String()
	} else {
		poolDSN += " search_path=" + schema
	}
	t.Setenv("PG_DSN", poolDSN)
	cfg, err := env.NewPGConfig()
	require.NoError(t, err)
	client, err := pg.NewPGClient(ctx, logger.With(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	files, err := filepath.Glob("../../../migrations/*.up.sql")
	require.NoError(t, err)
	for _, file := range files {
		sql, err := os.ReadFile(file)
		require.NoError(t, err)
		_, err = client.DB().ExecContext(ctx, db.Query{Name: "test:migrate", QueryRaw: string(sql)})
		require.NoError(t, err, file)
	}
	s := stand{
		tx:      transaction.NewTransactionManager(client.DB()),
		plans:   planRepository.NewRepository(client),
		actions: actionRepository.NewRepository(client),
	}
	settings := settingsRepository.NewRepository(client)
	require.NoError(t, settings.EnsureDefaults(ctx, model.MigrationSettings{BudgetBytes: 1 << 30, BudgetWindow: time.Hour, MinInterval: time.Hour}))
	require.NoError(t, s.plans.EnsureSettings(ctx, model.PlannerSettings{
		Horizon: time.Hour, HotFixedMs: 1, HotMiBPerMs: 1, ColdFixedMs: 20, ColdMiBPerMs: 0.1, HotUtilization: 0.9, ColdUtilization: 0.9,
	}))
	s.migrations = migrationService.NewService(s.actions, settings, pinRepository.NewRepository(client), s.tx, nil)
	s.planner = plannerService.NewService(plannerService.Config{Interval: time.Hour, KeepPlans: 2, MaxRejectedSaved: 10},
		s.plans, nil, s.actions, s.tx, nil, s.migrations, nil, ewma.NewPredictor())
	return s
}

// finished stores a ready plan whose promotion depends on an eviction, plus a
// rejected candidate.
func (s stand) finished(t *testing.T) (model.Plan, []model.Recommendation) {
	t.Helper()
	ctx := context.Background()
	plan, err := s.plans.CreatePlan(ctx, model.Plan{PlanID: uuid.NewString(), Trigger: model.TriggerTimer, MethodVersion: ewma.MethodVersion,
		Params: model.PlannerParams{Horizon: time.Hour}})
	require.NoError(t, err)
	require.Equal(t, model.PlanComputing, plan.Status)
	evict, promote, rejected := uuid.NewString(), uuid.NewString(), uuid.NewString()
	recs := []model.Recommendation{
		{RecommendationID: evict, PlanID: plan.PlanID, Order: 0, Kind: model.KindEvict, Selected: true, VersionID: uuid.NewString(),
			Bucket: "b", Key: "cold", ExpectedGeneration: 1, SourceClass: model.StorageClassHot, TargetClass: model.StorageClassCold, SizeBytes: 10,
			Explanation: model.Explanation{Effect: model.EffectExplanation{GainMs: 1, Set: "s"}}},
		{RecommendationID: promote, PlanID: plan.PlanID, Order: 1, Kind: model.KindPromote, Selected: true, VersionID: uuid.NewString(),
			Bucket: "b", Key: "hot", ExpectedGeneration: 2, SourceClass: model.StorageClassCold, TargetClass: model.StorageClassHot, SizeBytes: 10,
			DependsOn: []string{evict}, Explanation: model.Explanation{Effect: model.EffectExplanation{GainMs: 90, Set: "s"}, DependsOn: []string{evict}}},
		{RecommendationID: rejected, PlanID: plan.PlanID, Order: 2, Kind: model.KindPromote, Reason: model.RejectBudget, VersionID: uuid.NewString(),
			Bucket: "b", Key: "warm", ExpectedGeneration: 1, SourceClass: model.StorageClassCold, TargetClass: model.StorageClassHot, SizeBytes: 10,
			Explanation: model.Explanation{Reason: model.RejectBudget}},
	}
	plan.Status, plan.Summary = model.PlanReady, model.PlanSummary{Actions: 2, CopyBytes: 20, NetGainMs: 80}
	require.NoError(t, s.tx.ReadCommitted(ctx, func(ctx context.Context) error { return s.plans.FinishPlan(ctx, plan, recs) }))
	stored, err := s.plans.GetPlan(ctx, plan.PlanID, false)
	require.NoError(t, err)
	return stored, recs
}

func command(t model.CommandType, plan string, ids ...string) model.CommandRequest {
	return model.CommandRequest{CommandID: uuid.NewString(), Initiator: "alice", Type: t, PlanID: plan, RecommendationIDs: ids}
}

func TestPlanStorage(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	first, recs := s.finished(t)
	require.Equal(t, model.PlanReady, first.Status)
	require.Equal(t, 80.0, first.Summary.NetGainMs)

	got, err := s.plans.ListRecommendations(ctx, model.RecommendationFilter{PlanID: first.PlanID, PageSize: 2})
	require.NoError(t, err)
	require.Equal(t, recs[:2], got)
	rest, err := s.plans.ListRecommendations(ctx, model.RecommendationFilter{PlanID: first.PlanID, PageSize: 2, PageToken: "1"})
	require.NoError(t, err)
	require.Equal(t, recs[2:], rest)
	selected := false
	unselected, err := s.plans.ListRecommendations(ctx, model.RecommendationFilter{PlanID: first.PlanID, Selected: &selected, PageSize: 10})
	require.NoError(t, err)
	require.Equal(t, recs[2:], unselected)

	second, _ := s.finished(t)
	first, err = s.plans.GetPlan(ctx, first.PlanID, false)
	require.NoError(t, err)
	require.Equal(t, model.PlanSuperseded, first.Status, "a newer revision replaces a ready plan")
	require.Greater(t, second.Revision, first.Revision)
	latest, err := s.plans.LatestPlan(ctx)
	require.NoError(t, err)
	require.Equal(t, second.PlanID, latest.PlanID)

	// KeepPlans is 2: the oldest unconfirmed plan and its recommendations go.
	s.finished(t)
	require.NoError(t, s.plans.PrunePlans(ctx, 2))
	_, err = s.plans.GetPlan(ctx, first.PlanID, false)
	require.ErrorIs(t, err, model.ErrPlanNotFound)
}

func TestConfirmSubmitsToTheExecutor(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	plan, recs := s.finished(t)

	partial, err := s.planner.Execute(ctx, command(model.CommandConfirm, plan.PlanID, recs[1].RecommendationID))
	require.NoError(t, err)
	require.Equal(t, "DEPENDENCY_REQUIRED", partial.Code)
	require.Equal(t, []string{recs[0].RecommendationID}, partial.Missing)

	req := command(model.CommandConfirm, plan.PlanID)
	cmd, err := s.planner.Execute(ctx, req)
	require.NoError(t, err)
	require.Equal(t, model.CommandAccepted, cmd.Status)
	page, err := s.migrations.ListActions(ctx, model.ActionFilter{PlanID: plan.PlanID})
	require.NoError(t, err)
	require.Len(t, page.Actions, 2)
	promoted, err := s.actions.Get(ctx, recs[1].RecommendationID)
	require.NoError(t, err)
	require.Equal(t, []string{recs[0].RecommendationID}, promoted.DependsOn)
	require.True(t, promoted.DepsPending)

	again, err := s.planner.Execute(ctx, req)
	require.NoError(t, err)
	require.Equal(t, cmd.Actions, again.Actions, "a retried command returns its first result")

	// A newer revision does not unconfirm this plan, but it can no longer be confirmed.
	s.finished(t)
	stale, err := s.planner.Execute(ctx, command(model.CommandConfirm, plan.PlanID))
	require.NoError(t, err)
	require.Equal(t, "PLAN_STALE", stale.Code)
	stored, err := s.plans.GetPlan(ctx, plan.PlanID, false)
	require.NoError(t, err)
	require.Equal(t, model.PlanConfirmed, stored.Status)

	journal, err := s.planner.ListCommands(ctx, model.CommandFilter{PlanID: plan.PlanID, PageSize: 2})
	require.NoError(t, err)
	require.Len(t, journal.Commands, 2)
	require.NotEmpty(t, journal.NextPageToken)
	older, err := s.planner.ListCommands(ctx, model.CommandFilter{PlanID: plan.PlanID, PageSize: 2, PageToken: journal.NextPageToken})
	require.NoError(t, err)
	require.Len(t, older.Commands, 1)
	require.Equal(t, "DEPENDENCY_REQUIRED", older.Commands[0].Code)
}

// The executor refusing the actions must leave neither a confirmed plan nor a
// journaled command behind.
func TestConfirmIsAtomic(t *testing.T) {
	s := database(t)
	ctx := context.Background()
	plan, recs := s.finished(t)
	conflicting := model.ActionSpec{ActionID: recs[0].RecommendationID, PlanID: uuid.NewString(), VersionID: uuid.NewString(),
		ExpectedGeneration: 1, SourceClass: model.StorageClassHot, TargetClass: model.StorageClassCold, SizeBytes: 99}
	_, err := s.migrations.SubmitActions(ctx, conflicting.PlanID, []model.ActionSpec{conflicting})
	require.NoError(t, err)

	req := command(model.CommandConfirm, plan.PlanID)
	_, err = s.planner.Execute(ctx, req)
	require.ErrorIs(t, err, model.ErrActionConflict)
	stored, err := s.plans.GetPlan(ctx, plan.PlanID, false)
	require.NoError(t, err)
	require.Equal(t, model.PlanReady, stored.Status)
	_, err = s.plans.GetCommand(ctx, req.CommandID)
	require.ErrorIs(t, err, model.ErrCommandNotFound)
	_, err = s.actions.Get(ctx, recs[1].RecommendationID)
	require.ErrorIs(t, err, model.ErrActionNotFound)
}
