//go:build integration

package action_test

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
	settingsRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/settings"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/pg"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/transaction"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

type store struct {
	client   db.Client
	tx       db.TxManager
	actions  repository.ActionRepository
	settings repository.SettingsRepository
}

// database applies every SPPR migration to a fresh schema of SPPR_TEST_PG_DSN.
func database(t *testing.T) store {
	t.Helper()
	dsn := os.Getenv("SPPR_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set SPPR_TEST_PG_DSN to a test PostgreSQL database")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	schema := fmt.Sprintf("sppr_migration_test_%d", time.Now().UnixNano())
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
	s := store{
		client:   client,
		tx:       transaction.NewTransactionManager(client.DB()),
		actions:  actionRepository.NewRepository(client),
		settings: settingsRepository.NewRepository(client),
	}
	require.NoError(t, s.settings.EnsureDefaults(ctx, model.MigrationSettings{
		BudgetBytes: 1 << 30, BudgetWindow: time.Hour, MinInterval: time.Hour,
	}))
	return s
}

func spec(plan string, size int64, deps ...string) model.ActionSpec {
	return model.ActionSpec{
		ActionID:           uuid.NewString(),
		PlanID:             plan,
		VersionID:          uuid.NewString(),
		ExpectedGeneration: 1,
		SourceClass:        model.StorageClassCold,
		TargetClass:        model.StorageClassHot,
		SizeBytes:          size,
		DependsOn:          deps,
	}
}

func (s store) insert(t *testing.T, specs ...model.ActionSpec) {
	t.Helper()
	require.NoError(t, s.tx.ReadCommitted(context.Background(), func(ctx context.Context) error {
		return s.actions.Insert(ctx, specs)
	}))
}

// claim takes an action the way the executor does: under the settings lock,
// within the remaining budget.
func (s store) claim(t *testing.T) *model.Action {
	t.Helper()
	var claimed *model.Action
	require.NoError(t, s.tx.ReadCommitted(context.Background(), func(ctx context.Context) error {
		settings, err := s.settings.GetForUpdate(ctx)
		if err != nil {
			return err
		}
		spent, err := s.settings.Spent(ctx, settings.BudgetWindow)
		if err != nil {
			return err
		}
		claimed, err = s.actions.Claim(ctx, settings.BudgetBytes-spent)
		return err
	}))
	return claimed
}

func (s store) transition(t *testing.T, tr model.Transition) []string {
	t.Helper()
	var dependents []string
	require.NoError(t, s.tx.ReadCommitted(context.Background(), func(ctx context.Context) error {
		var err error
		dependents, err = s.actions.Transition(ctx, tr)
		return err
	}))
	return dependents
}

func (s store) get(t *testing.T, id string) *model.Action {
	t.Helper()
	a, err := s.actions.Get(context.Background(), id)
	require.NoError(t, err)
	return a
}
