//go:build integration

package modelversion_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository/modelversion"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository/plan"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/pg"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/transaction"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func database(t *testing.T) db.Client {
	t.Helper()
	dsn := os.Getenv("SPPR_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set SPPR_TEST_PG_DSN to a test PostgreSQL database")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	schema := fmt.Sprintf("sppr_admin_form_%d", time.Now().UnixNano())
	_, err = conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		require.NoError(t, err)
	})
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}
	t.Setenv("PG_DSN", dsn)
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
		require.NoError(t, err)
	}
	return client
}

func TestModelJournalAtomicActivation(t *testing.T) {
	logger.SetNopLogger()
	client := database(t)
	ctx := context.Background()
	repo := modelversion.New(client, transaction.NewTransactionManager(client.DB()))
	now := time.Now().UTC().Truncate(time.Microsecond)
	from, to := now.Add(-48*time.Hour), now.Add(-time.Hour)
	score := 0.3
	for _, id := range []string{"A", "B", "C"} {
		require.NoError(t, repo.Save(ctx, model.ModelVersion{ModelVersion: id, Status: "candidate", Source: "manual", CreatedAt: now, TrainedFrom: &from, TrainedTo: &to, TrainRows: 12, TrainSeconds: 1.2, ValidationCovK: &score}))
	}
	require.NoError(t, repo.Activate(ctx, "A", now))
	require.Error(t, repo.Activate(ctx, "missing", now))
	rows, e := repo.List(ctx, time.Time{}, 100)
	require.NoError(t, e)
	active := 0
	for _, v := range rows {
		if v.Status == "active" {
			active++
			require.Equal(t, "A", v.ModelVersion)
			require.EqualValues(t, 12, v.TrainRows)
		}
	}
	require.Equal(t, 1, active)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, id := range []string{"B", "C"} {
		wg.Add(1)
		go func(id string) { defer wg.Done(); errs <- repo.Activate(ctx, id, now.Add(time.Second)) }(id)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	rows, e = repo.List(ctx, time.Time{}, 100)
	require.NoError(t, e)
	active = 0
	for _, v := range rows {
		if v.Status == "active" {
			active++
			require.NoError(t, repo.Retire(ctx, v.ModelVersion, "model_stale", now.Add(2*time.Second)))
		}
	}
	require.Equal(t, 1, active)
	require.NoError(t, repo.Save(ctx, model.ModelVersion{ModelVersion: "failed-attempt", Status: "candidate", Source: "scheduled", CreatedAt: now.Add(3 * time.Second)}))
	require.NoError(t, repo.FailInterrupted(ctx))
	rows, e = repo.List(ctx, time.Time{}, 100)
	require.NoError(t, e)
	require.Equal(t, "training_interrupted", rows[0].Reason)
	// The migrated admin journal accepts retrain and reconstructs its request.
	commands := plan.NewRepository(client)
	req := model.CommandRequest{CommandID: uuid.NewString(), Type: model.CommandRetrain, Initiator: uuid.NewString()}
	require.NoError(t, commands.InsertCommand(ctx, model.Command{CommandRequest: req, Status: model.CommandAccepted, CreatedAt: now}))
	stored, e := commands.GetCommand(ctx, req.CommandID)
	require.NoError(t, e)
	require.Equal(t, req, stored.CommandRequest)
	// Roll back the migration after removing its rows and ensure the old command constraint is restored.
	sql, e := os.ReadFile("../../../migrations/20261102000000_model_versions.down.sql")
	require.NoError(t, e)
	_, e = client.DB().ExecContext(ctx, db.Query{Name: "test:down", QueryRaw: string(sql)})
	require.NoError(t, e)
	require.Error(t, commands.InsertCommand(ctx, model.Command{CommandRequest: req, Status: model.CommandAccepted, CreatedAt: now}))
}
