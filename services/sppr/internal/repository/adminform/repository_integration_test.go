//go:build integration

package adminform_test

import (
	"context"
	"errors"
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
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository/adminform"
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
	_, err = client.DB().ExecContext(ctx, db.Query{Name: "test:effects", QueryRaw: `CREATE TABLE test_effects(id int PRIMARY KEY, count int NOT NULL); INSERT INTO test_effects VALUES(1,0)`})
	require.NoError(t, err)
	return client
}
func TestAtomicFormCommands(t *testing.T) {
	logger.SetNopLogger()
	client := database(t)
	ctx := context.Background()
	repo := adminform.New(client, transaction.NewTransactionManager(client.DB()))
	effect := func(ctx context.Context) error {
		_, err := client.DB().ExecContext(ctx, db.Query{Name: "test:effect", QueryRaw: `UPDATE test_effects SET count=count+1 WHERE id=1`})
		return err
	}
	req := model.CommandRequest{CommandID: uuid.NewString(), Initiator: uuid.NewString(), Type: "budget"}
	const workers = 4
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := repo.Execute(ctx, req, "budget=1024", effect); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var count int
	require.NoError(t, client.DB().QueryRowContext(ctx, db.Query{Name: "test:count", QueryRaw: `SELECT count FROM test_effects WHERE id=1`}).Scan(&count))
	require.Equal(t, 1, count)
	restarted := adminform.New(client, transaction.NewTransactionManager(client.DB()))
	cmd, err := restarted.Execute(ctx, req, "budget=1024", func(context.Context) error { t.Fatal("replay applied twice"); return nil })
	require.NoError(t, err)
	require.Equal(t, model.CommandAccepted, cmd.Status)
	_, err = repo.Execute(ctx, req, "budget=2048", effect)
	require.ErrorIs(t, err, model.ErrCommandConflict)
	req.CommandID = uuid.NewString()
	refused, err := repo.Execute(ctx, req, "pin", func(context.Context) error { return model.ErrPinExists })
	require.NoError(t, err)
	require.Equal(t, "PIN_EXISTS", refused.Code)
	replay, err := repo.Execute(ctx, req, "pin", effect)
	require.NoError(t, err)
	require.Equal(t, refused, replay)
	req.CommandID = uuid.NewString()
	boom := errors.New("infrastructure failed")
	_, err = repo.Execute(ctx, req, "fail", func(ctx context.Context) error {
		if err := effect(ctx); err != nil {
			return err
		}
		return boom
	})
	require.ErrorIs(t, err, boom)
	_, err = repo.Get(ctx, req.CommandID)
	require.ErrorIs(t, err, model.ErrCommandNotFound)
	require.NoError(t, client.DB().QueryRowContext(ctx, db.Query{Name: "test:count", QueryRaw: `SELECT count FROM test_effects WHERE id=1`}).Scan(&count))
	require.Equal(t, 1, count)
	page, err := repo.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, page.Commands, 2)
	// Multiple pages must be disjoint and complete, including timestamp ties.
	for i := 0; i < 51; i++ {
		req.CommandID = uuid.NewString()
		_, err := repo.Execute(ctx, req, "page", func(context.Context) error { return nil })
		require.NoError(t, err)
	}
	seen := map[string]bool{}
	token := ""
	for {
		page, err := repo.List(ctx, token)
		require.NoError(t, err)
		for _, cmd := range page.Commands {
			require.False(t, seen[cmd.CommandID], "duplicate row across pages")
			seen[cmd.CommandID] = true
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Len(t, seen, 53)
}
