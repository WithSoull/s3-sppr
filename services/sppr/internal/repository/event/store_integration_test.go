//go:build integration

package event_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	eventRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/event"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/ingestor"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/pg"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/transaction"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func database(t *testing.T) (db.Client, service.Ingestor) {
	t.Helper()
	dsn := os.Getenv("SPPR_TEST_PG_DSN")
	require.NotEmpty(t, dsn, "set SPPR_TEST_PG_DSN to a test PostgreSQL database")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	schema := fmt.Sprintf("sppr_test_%d", time.Now().UnixNano())
	_, err = conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		require.NoError(t, err)
	})
	// Preserve URL and keyword DSNs without concatenating unescaped values.
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
	sql, err := os.ReadFile("../../../migrations/20261021000000_ingestion.up.sql")
	require.NoError(t, err)
	_, err = client.DB().ExecContext(ctx, db.Query{Name: "test:migrate", QueryRaw: string(sql)})
	require.NoError(t, err)
	return client, ingestor.NewService(eventRepository.NewRepository(client), transaction.NewTransactionManager(client.DB()))
}

func accessEvent(sequence int64) model.AccessEvent {
	return model.AccessEvent{SchemaVersion: 1, GatewayID: "gw", StreamID: "run", Sequence: sequence,
		OccurredAt: time.Date(2026, 10, 21, 12, 0, int(sequence), 0, time.UTC), Bucket: "b", ObjectKey: "k", VersionID: "v1", Operation: "read", Origin: "client", Result: "success", Bytes: 100}
}

func deliver(t *testing.T, s service.Ingestor, e model.AccessEvent, offset int64) model.IngestResult {
	t.Helper()
	r, err := s.Ingest(context.Background(), e, model.Delivery{Topic: "access", Partition: 0, Offset: offset})
	require.NoError(t, err)
	return r
}

func scalar(t *testing.T, client db.Client, sql string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, client.DB().QueryRowContext(context.Background(), db.Query{Name: "test:query", QueryRaw: sql}).Scan(&n))
	return n
}

func TestStoreDeduplicationGapsAndDemand(t *testing.T) {
	client, s := database(t)
	deliver(t, s, accessEvent(1), 0)
	// Rebuild service as after restart, replaying a committed but unacknowledged event.
	s = ingestor.NewService(eventRepository.NewRepository(client), transaction.NewTransactionManager(client.DB()))
	require.True(t, deliver(t, s, accessEvent(1), 0).Duplicate)
	require.Equal(t, int64(1), deliver(t, s, accessEvent(3), 2).Missing)
	migration := accessEvent(4)
	migration.Origin = "migration"
	deliver(t, s, migration, 3)
	write := accessEvent(5)
	write.Operation = "write"
	deliver(t, s, write, 4)
	failure := accessEvent(6)
	failure.Result = "error"
	failure.VersionID = ""
	deliver(t, s, failure, 5)
	require.Equal(t, int64(5), scalar(t, client, "SELECT count(*) FROM access_events"))
	require.Equal(t, int64(2), scalar(t, client, "SELECT sum(reads) FROM read_aggregates"))
	require.Equal(t, int64(200), scalar(t, client, "SELECT sum(bytes) FROM read_aggregates"))
	require.Equal(t, int64(1), scalar(t, client, "SELECT missing_events FROM ingestion_streams"))
	require.Equal(t, int64(2), scalar(t, client, "SELECT first_sequence FROM ingestion_gaps"))
	require.Equal(t, int64(2), scalar(t, client, "SELECT last_sequence FROM ingestion_gaps"))
}

func TestStoreRollsBackEventGapAndProgressWhenAggregateFails(t *testing.T) {
	client, s := database(t)
	deliver(t, s, accessEvent(1), 0)
	_, err := client.DB().ExecContext(context.Background(), db.Query{Name: "test:constraint", QueryRaw: "ALTER TABLE read_aggregates ADD CONSTRAINT test_limit CHECK (reads<=1)"})
	require.NoError(t, err)
	_, err = s.Ingest(context.Background(), accessEvent(3), model.Delivery{Topic: "access", Offset: 2})
	require.Error(t, err)
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM access_events"))
	require.Equal(t, int64(1), scalar(t, client, "SELECT last_sequence FROM ingestion_streams"))
	require.Equal(t, int64(0), scalar(t, client, "SELECT count(*) FROM ingestion_gaps"))
	_, err = client.DB().ExecContext(context.Background(), db.Query{Name: "test:unblock", QueryRaw: "ALTER TABLE read_aggregates DROP CONSTRAINT test_limit"})
	require.NoError(t, err)
	require.Equal(t, int64(1), deliver(t, s, accessEvent(3), 2).Missing)
	require.Equal(t, int64(2), scalar(t, client, "SELECT sum(reads) FROM read_aggregates"))
}

func TestStoreConcurrentRedeliveryCountsOnce(t *testing.T) {
	client, s := database(t)
	var wg sync.WaitGroup
	errors := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Ingest(context.Background(), accessEvent(1), model.Delivery{Topic: "access", Offset: 0})
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM access_events"))
	require.Equal(t, int64(1), scalar(t, client, "SELECT sum(reads) FROM read_aggregates"))
}

func TestStoreRejectsStreamContractViolations(t *testing.T) {
	cases := []struct {
		name   string
		change func(*model.AccessEvent, *model.Delivery)
	}{
		{name: "identity collision", change: func(e *model.AccessEvent, _ *model.Delivery) { e.Sequence = 1; e.Bytes = 999 }},
		{name: "partition change", change: func(_ *model.AccessEvent, d *model.Delivery) { d.Partition = 1 }},
		{name: "event time regression", change: func(e *model.AccessEvent, _ *model.Delivery) { e.OccurredAt = e.OccurredAt.Add(-time.Hour) }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			client, s := database(t)
			deliver(t, s, accessEvent(1), 0)
			e := accessEvent(2)
			d := model.Delivery{Topic: "access", Offset: 1}
			tt.change(&e, &d)
			_, err := s.Ingest(context.Background(), e, d)
			require.Error(t, err)
			require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM access_events"))
			require.Equal(t, int64(1), scalar(t, client, "SELECT last_sequence FROM ingestion_streams"))
		})
	}
}

func TestStoreFirstGapAndIndependentRestart(t *testing.T) {
	client, s := database(t)
	require.Equal(t, int64(4), deliver(t, s, accessEvent(5), 4).Missing)
	require.Equal(t, int64(1), scalar(t, client, "SELECT count(*) FROM ingestion_gaps WHERE after_time IS NULL"))
	restarted := accessEvent(1)
	restarted.StreamID = "next-run"
	deliver(t, s, restarted, 5)
	require.Equal(t, int64(2), scalar(t, client, "SELECT count(*) FROM ingestion_streams"))
	require.Equal(t, int64(2), scalar(t, client, "SELECT sum(reads) FROM read_aggregates"))
}
