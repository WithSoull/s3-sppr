package object

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/repository"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
)

type objectService struct {
	repo               repository.ObjectRepository
	placementRepo      repository.PlacementRepository
	copyRepo           repository.CopyRepository
	txManager          db.TxManager
	objectDeleted      kafka.Producer
	deleteCleanupDelay time.Duration
	pgClient           db.Client
	saramaClient       sarama.Client
}

func NewService(
	repo repository.ObjectRepository,
	placementRepo repository.PlacementRepository,
	copyRepo repository.CopyRepository,
	txManager db.TxManager,
	objectDeleted kafka.Producer,
	deleteCleanupDelay time.Duration,
	pgClient db.Client,
	saramaClient sarama.Client,
) service.ObjectService {
	return &objectService{
		repo:               repo,
		placementRepo:      placementRepo,
		copyRepo:           copyRepo,
		txManager:          txManager,
		objectDeleted:      objectDeleted,
		deleteCleanupDelay: deleteCleanupDelay,
		pgClient:           pgClient,
		saramaClient:       saramaClient,
	}
}
