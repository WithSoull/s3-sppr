package placement

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
)

func (s *placementService) GetPlacement(ctx context.Context, versionID string) (*model.Placement, error) {
	if err := validate.Validate(ctx, uuidField("version_id", versionID)); err != nil {
		return nil, err
	}

	return s.placementRepo.Get(ctx, versionID)
}

func (s *placementService) ListMigrationCopies(ctx context.Context, migrationID string) ([]*model.PlacementCopy, error) {
	if err := validate.Validate(ctx, uuidField("migration_id", migrationID)); err != nil {
		return nil, err
	}

	return s.copyRepo.ListByMigration(ctx, migrationID)
}

func (s *placementService) ListDueCleanups(ctx context.Context, limit int32) ([]*model.PlacementCopy, error) {
	return s.copyRepo.ListDueCleanups(ctx, limit)
}
