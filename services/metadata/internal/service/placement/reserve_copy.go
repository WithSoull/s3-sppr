package placement

import (
	"context"
	"errors"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
)

func (s *placementService) ReserveCopy(
	ctx context.Context,
	migrationID, versionID string,
	expectedGeneration int64,
	sourceClass, targetClass model.StorageClass,
) (*model.PlacementCopy, error) {
	if err := validate.Validate(ctx,
		uuidField("migration_id", migrationID),
		uuidField("version_id", versionID),
		func(context.Context) error {
			if expectedGeneration <= 0 {
				return validate.NewValidationErrors("expected_generation must be positive")
			}
			return nil
		},
		storageClassField("source_class", sourceClass),
		storageClassField("target_class", targetClass),
		func(context.Context) error {
			if sourceClass == targetClass {
				return validate.NewValidationErrors("target_class must differ from source_class")
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	var reserved *model.PlacementCopy
	txErr := s.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		// A retried command must not start a second migration: return what the
		// first attempt reserved, whatever state it has reached since.
		existing, err := s.copyRepo.GetByMigration(ctx, migrationID, model.CopyRoleTarget)
		if err == nil {
			if (existing.VersionID != nil && *existing.VersionID != versionID) || existing.StorageClass != targetClass {
				return domainerrors.ErrMigrationMismatch
			}
			reserved = existing
			return nil
		}
		if !errors.Is(err, domainerrors.ErrCopyNotFound) {
			return err
		}

		placement, err := s.placementRepo.LockForChange(ctx, versionID)
		if err != nil {
			return err
		}
		if !placement.ObjectActive || (!placement.IsCurrent && targetClass != model.StorageClassCold) {
			return domainerrors.ErrVersionNotCurrent
		}
		if placement.Generation != expectedGeneration || placement.StorageClass != sourceClass {
			return domainerrors.ErrPlacementChanged
		}

		if err := s.placementRepo.LockStorageClass(ctx, targetClass); err != nil {
			return err
		}
		usage, err := s.placementRepo.GetUsage(ctx, targetClass)
		if err != nil {
			return err
		}
		if usage.FreeBytes() < placement.SizeBytes {
			return domainerrors.ErrInsufficientCapacity
		}

		reserved, err = s.copyRepo.InsertTarget(ctx, migrationID, placement, targetClass)
		return err
	})
	if txErr != nil {
		return nil, txErr
	}

	return reserved, nil
}
