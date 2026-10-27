//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	e2esuite "github.com/alesplll/opens3-rebac/e2e/suite"
	e2econfig "github.com/alesplll/opens3-rebac/e2e/suite/config"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/repository"
	objectRepo "github.com/alesplll/opens3-rebac/services/metadata/internal/repository/object"
	placementRepo "github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placement"
	placementCopyRepo "github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placementcopy"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/service"
	objectService "github.com/alesplll/opens3-rebac/services/metadata/internal/service/object"
	placementService "github.com/alesplll/opens3-rebac/services/metadata/internal/service/placement"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	pgclient "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/pg"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/transaction"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type testLogger struct{}

func (testLogger) Debug(context.Context, string, ...zap.Field) {}

// discardProducer stands in for Kafka: these tests check rows, not events.
type discardProducer struct{}

func (discardProducer) Send(context.Context, []byte, []byte, kafka.PrettyDecoder) error { return nil }

// deleteCleanupDelay is long enough that only expireCleanupDelays makes orphan
// copies due.
const deleteCleanupDelay = time.Hour

// PlacementProtocolSuite runs the migration protocol against real PostgreSQL:
// row locks, CAS and capacity accounting cannot be checked with mocks.
type PlacementProtocolSuite struct {
	e2esuite.BasePostgresSuite

	client     db.Client
	objects    repository.ObjectRepository
	objectSvc  service.ObjectService
	placements service.PlacementService
}

func TestPlacementProtocolSuite(t *testing.T) {
	suite.Run(t, new(PlacementProtocolSuite))
}

func (s *PlacementProtocolSuite) SetupSuite() {
	s.SetupPostgresSuite(e2esuite.PostgresSuiteOptions{
		MigrationDir:  s.MustServiceMigrationDir("metadata"),
		CleanupTables: []string{"placement_copies", "version_placements", "versions", "objects", "buckets"},
		ResolvePGConfig: func(cfg *e2econfig.Config) e2econfig.PGConfig {
			return cfg.MetadataPG
		},
	})

	client, err := pgclient.NewPGClient(s.Context(), testLogger{}, s.PGConfig())
	s.Require().NoError(err)

	txManager := transaction.NewTransactionManager(client.DB())
	placements := placementRepo.NewRepository(client)
	copies := placementCopyRepo.NewRepository(client)

	s.client = client
	s.objects = objectRepo.NewRepository(client)
	s.objectSvc = objectService.NewService(s.objects, placements, copies, txManager, discardProducer{}, deleteCleanupDelay, nil, nil)
	s.placements = placementService.NewService(placements, copies, txManager)
}

func (s *PlacementProtocolSuite) SetupTest() {
	s.BasePostgresSuite.SetupTest()
	s.setCapacity("HOT", 1_000_000)
	s.setCapacity("COLD", 1_000_000)
}

func (s *PlacementProtocolSuite) TearDownSuite() {
	if s.client != nil {
		_ = s.client.Close()
	}

	s.BasePostgresSuite.TearDownSuite()
}
