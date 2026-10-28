package planner

import (
	"sync"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/google/uuid"
)

const (
	defaultPageSize = 100
	maxPageSize     = 100
	// recomputeQueue only needs to hold one pending cycle; extra commands in a
	// burst attach to the same plan.
	recomputeQueue = 64
)

type Config struct {
	Models           service.ModelLifecycle
	MaxHistoryLag    time.Duration
	FallbackReason   string
	Interval         time.Duration
	KeepPlans        int
	MaxRejectedSaved int
}

type plannerService struct {
	commandMu  sync.Mutex
	cfg        Config
	plans      repository.PlanRepository
	history    repository.HistoryRepository
	actions    repository.ActionRepository
	tx         db.TxManager
	placements client.PlacementClient
	migrations service.MigrationService
	features   service.FeatureBuilder
	predictor  service.Predictor
	recompute  chan string
	now        func() time.Time
	newID      func() string
}

func NewService(
	cfg Config,
	plans repository.PlanRepository,
	history repository.HistoryRepository,
	actions repository.ActionRepository,
	tx db.TxManager,
	placements client.PlacementClient,
	migrations service.MigrationService,
	features service.FeatureBuilder,
	predictor service.Predictor,
) service.PlannerService {
	return newService(cfg, plans, history, actions, tx, placements, migrations, features, predictor, time.Now, uuid.NewString)
}

func newService(
	cfg Config,
	plans repository.PlanRepository,
	history repository.HistoryRepository,
	actions repository.ActionRepository,
	tx db.TxManager,
	placements client.PlacementClient,
	migrations service.MigrationService,
	features service.FeatureBuilder,
	predictor service.Predictor,
	now func() time.Time,
	newID func() string,
) *plannerService {
	if cfg.MaxHistoryLag <= 0 {
		cfg.MaxHistoryLag = 60 * time.Second
	}
	return &plannerService{
		cfg: cfg, plans: plans, history: history, actions: actions, tx: tx, placements: placements,
		migrations: migrations, features: features, predictor: predictor,
		recompute: make(chan string, recomputeQueue), now: now, newID: newID,
	}
}

func pageSize(n int) int {
	if n <= 0 {
		return defaultPageSize
	}
	return min(n, maxPageSize)
}
