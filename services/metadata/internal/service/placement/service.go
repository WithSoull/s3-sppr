package placement

import (
	"github.com/alesplll/opens3-rebac/services/metadata/internal/repository"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

type placementService struct {
	placementRepo repository.PlacementRepository
	copyRepo      repository.CopyRepository
	txManager     db.TxManager
}

func NewService(
	placementRepo repository.PlacementRepository,
	copyRepo repository.CopyRepository,
	txManager db.TxManager,
) service.PlacementService {
	return &placementService{
		placementRepo: placementRepo,
		copyRepo:      copyRepo,
		txManager:     txManager,
	}
}
