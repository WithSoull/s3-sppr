package placement

import (
	"github.com/alesplll/opens3-rebac/services/metadata/internal/repository"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

type repo struct {
	db db.Client
}

func NewRepository(dbClient db.Client) repository.PlacementRepository {
	return &repo{db: dbClient}
}
