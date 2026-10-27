//go:build integration

package executor_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	metadataClient "github.com/alesplll/opens3-rebac/services/sppr/internal/client/metadata"
	storageClient "github.com/alesplll/opens3-rebac/services/sppr/internal/client/storage"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	actionRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/action"
	pinRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/pin"
	settingsRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/settings"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/cleanup"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/executor"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/migration"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/pg"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/transaction"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type stand struct {
	t                    *testing.T
	ctx                  context.Context
	actions              repository.ActionRepository
	settings             repository.SettingsRepository
	pins                 repository.PinRepository
	tx                   db.TxManager
	api                  service.MigrationService
	placements           client.PlacementClient
	storages             map[model.StorageClass]client.StorageClient
	meta                 metadatav1.MetadataServiceClient
	placementAPI         metadatav1.PlacementServiceClient
	cfg                  executor.Config
	gateway, bucket, key string
	body                 []byte
	spec                 model.ActionSpec
}

func standEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func newStand(t *testing.T) *stand {
	t.Helper()
	if os.Getenv("SPPR_TEST_MIGRATION_STAND") != "1" {
		t.Skip("set SPPR_TEST_MIGRATION_STAND=1 for the Gateway/Metadata/Storage stand")
	}
	dsn := os.Getenv("SPPR_TEST_PG_DSN")
	require.NotEmpty(t, dsn, "SPPR_TEST_PG_DSN is required when the stand is enabled")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	schema := "sppr_stand_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := conn.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		require.NoError(t, err)
		require.NoError(t, conn.Close(context.Background()))
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
	pgCfg, err := env.NewPGConfig()
	require.NoError(t, err)
	database, err := pg.NewPGClient(ctx, logger.With(), pgCfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	files, err := filepath.Glob("../../../migrations/*.up.sql")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		sql, err := os.ReadFile(file)
		require.NoError(t, err)
		_, err = database.DB().ExecContext(ctx, db.Query{Name: "stand:migrate", QueryRaw: string(sql)})
		require.NoError(t, err)
	}
	dial := func(address string) *grpc.ClientConn {
		c, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		return c
	}
	metaConn := dial(standEnv("SPPR_TEST_METADATA_GRPC_ADDR", "localhost:50052"))
	s := &stand{t: t, ctx: ctx, actions: actionRepository.NewRepository(database), settings: settingsRepository.NewRepository(database),
		pins: pinRepository.NewRepository(database), tx: transaction.NewTransactionManager(database.DB()),
		placements: metadataClient.NewClient(metadatav1.NewPlacementServiceClient(metaConn), 3*time.Second),
		meta:       metadatav1.NewMetadataServiceClient(metaConn), placementAPI: metadatav1.NewPlacementServiceClient(metaConn),
		gateway: strings.TrimRight(standEnv("SPPR_TEST_GATEWAY_URL", "http://localhost:8080"), "/"),
		bucket:  "migration-stand-" + uuid.NewString(), key: "object.bin", body: bytes.Repeat([]byte("0123456789abcdef"), 8193),
		cfg: executor.Config{Workers: 1, MaxAttempts: 3, RetryBackoff: time.Millisecond, PollInterval: 5 * time.Millisecond, SourceCleanupDelay: 10 * time.Minute},
	}
	s.storages = map[model.StorageClass]client.StorageClient{
		model.StorageClassHot:  storageClient.NewClient(storagev1.NewDataStorageServiceClient(dial(standEnv("SPPR_TEST_STORAGE_HOT_GRPC_ADDR", "localhost:50056")))),
		model.StorageClassCold: storageClient.NewClient(storagev1.NewDataStorageServiceClient(dial(standEnv("SPPR_TEST_STORAGE_COLD_GRPC_ADDR", "localhost:50053")))),
	}
	require.NoError(t, s.settings.EnsureDefaults(ctx, model.MigrationSettings{BudgetBytes: 1 << 30, BudgetWindow: time.Hour}))
	s.api = migration.NewService(s.actions, s.settings, s.pins, s.tx, nil)
	_, err = s.meta.CreateBucket(ctx, &metadatav1.CreateBucketRequest{Name: s.bucket, OwnerId: uuid.NewString()})
	require.NoError(t, err)
	s.put(s.body)
	p := s.placement()
	require.Equal(t, model.StorageClassCold, p.Class)
	s.spec = model.ActionSpec{ActionID: uuid.NewString(), PlanID: uuid.NewString(), VersionID: p.VersionID, ExpectedGeneration: p.Generation,
		SourceClass: p.Class, TargetClass: model.StorageClassHot, SizeBytes: p.SizeBytes}
	return s
}

func (s *stand) request(method string, body []byte) *http.Response {
	s.t.Helper()
	req, err := http.NewRequestWithContext(s.ctx, method, s.gateway+"/objects/"+s.bucket+"/"+s.key, bytes.NewReader(body))
	require.NoError(s.t, err)
	response, err := http.DefaultClient.Do(req)
	require.NoError(s.t, err)
	return response
}
func (s *stand) put(body []byte) {
	s.t.Helper()
	resp := s.request(http.MethodPut, body)
	defer resp.Body.Close()
	require.Equal(s.t, http.StatusOK, resp.StatusCode)
}
func (s *stand) get() {
	s.t.Helper()
	resp := s.request(http.MethodGet, nil)
	defer resp.Body.Close()
	require.Equal(s.t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(s.t, err)
	require.Equal(s.t, s.body, body)
}
func (s *stand) placement() model.Placement {
	s.t.Helper()
	meta, err := s.meta.GetObjectMeta(s.ctx, &metadatav1.GetObjectMetaRequest{BucketName: s.bucket, Key: s.key})
	require.NoError(s.t, err)
	p, err := s.placements.GetPlacement(s.ctx, meta.GetVersionId())
	require.NoError(s.t, err)
	return p
}
func (s *stand) submit(specs ...model.ActionSpec) {
	s.t.Helper()
	if len(specs) == 0 {
		specs = []model.ActionSpec{s.spec}
	}
	_, err := s.api.SubmitActions(s.ctx, s.spec.PlanID, specs)
	require.NoError(s.t, err)
}
func (s *stand) executor() service.MigrationExecutor {
	return executor.NewExecutor(s.cfg, s.actions, s.settings, s.pins, s.tx, s.placements, s.storages, nil)
}
func (s *stand) step() {
	s.t.Helper()
	worked, err := s.executor().Step(s.ctx)
	require.NoError(s.t, err)
	require.True(s.t, worked)
}
func (s *stand) action() *model.Action {
	s.t.Helper()
	a, err := s.actions.Get(s.ctx, s.spec.ActionID)
	require.NoError(s.t, err)
	return a
}
func (s *stand) outcome(status model.ActionStatus, reason model.Reason) {
	s.t.Helper()
	a := s.action()
	require.Equal(s.t, status, a.Status)
	require.Equal(s.t, reason, a.Reason)
}
func (s *stand) copies() []*metadatav1.PlacementCopyInfo {
	s.t.Helper()
	resp, err := s.placementAPI.ListMigrationCopies(s.ctx, &metadatav1.ListMigrationCopiesRequest{MigrationId: s.spec.ActionID})
	require.NoError(s.t, err)
	return resp.GetCopies()
}
func (s *stand) target() *metadatav1.PlacementCopyInfo {
	s.t.Helper()
	for _, c := range s.copies() {
		if c.GetRole() == "target" {
			return c
		}
	}
	s.t.Fatal("missing target copy")
	return nil
}
func (s *stand) cleaner(copyID string) service.MigrationCleanup {
	// Metadata is shared by stand tests; never clean another test's or user's blobs.
	filtered := &placementFault{PlacementClient: s.placements, due: func(ctx context.Context, _ int32) ([]model.PlacementCopy, error) {
		copies, err := s.placements.ListDueCleanups(ctx, 1000)
		if err != nil {
			return nil, err
		}
		var own []model.PlacementCopy
		for _, c := range copies {
			if c.CopyID == copyID {
				own = append(own, c)
			}
		}
		return own, nil
	}}
	return cleanup.NewService(5*time.Millisecond, filtered, s.storages, nil)
}
