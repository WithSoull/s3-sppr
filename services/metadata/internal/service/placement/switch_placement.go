package placement

import (
	"context"
	"errors"
	"time"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
)

func (s *placementService) SwitchPlacement(
	ctx context.Context,
	migrationID string,
	verifiedSizeBytes int64,
	verifiedMD5 string,
	sourceCleanupDelay time.Duration,
) (*model.SwitchResult, error) {
	if err := validate.Validate(ctx,
		uuidField("migration_id", migrationID),
		func(context.Context) error {
			if !md5Pattern.MatchString(verifiedMD5) {
				return validate.NewValidationErrors("verified_md5 must be 32 lowercase hex characters")
			}
			return nil
		},
		func(context.Context) error {
			if sourceCleanupDelay <= 0 {
				return validate.NewValidationErrors("source_cleanup_delay must be positive")
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	var result *model.SwitchResult
	txErr := s.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		target, err := s.copyRepo.GetByMigration(ctx, migrationID, model.CopyRoleTarget)
		if err != nil {
			return err
		}

		// Delete locks the object before FK cleanup locks its copy rows. Take the
		// same order, then reload the target under lock in case abort/switch won.
		var placement *model.Placement
		if target.State == model.CopyStateReserved && target.VersionID != nil && verifiedSizeBytes == target.SizeBytes {
			placement, err = s.placementRepo.LockForChange(ctx, *target.VersionID)
			if err != nil && !errors.Is(err, domainerrors.ErrPlacementNotFound) {
				return err
			}
		}
		target, err = s.copyRepo.LockByMigration(ctx, migrationID, model.CopyRoleTarget)
		if err != nil {
			return err
		}

		switch target.State {
		case model.CopyStatePromoted:
			result, err = s.previousSwitch(ctx, target)
			return err
		case model.CopyStateReserved:
		default:
			return domainerrors.ErrInvalidCopyState
		}

		if target.VersionID == nil {
			return domainerrors.ErrVersionNotCurrent
		}
		if verifiedSizeBytes != target.SizeBytes {
			return domainerrors.ErrCopySizeMismatch
		}

		if placement == nil {
			return domainerrors.ErrVersionNotCurrent
		}
		if !placement.ObjectActive || (!placement.IsCurrent && target.StorageClass != model.StorageClassCold) {
			return domainerrors.ErrVersionNotCurrent
		}

		// CAS and InsertSource check FKs in target/source order. Take the same
		// ordered class locks as ReserveCopy before either FK can lock a class.
		if err := s.placementRepo.LockStorageClasses(ctx, placement.StorageClass, target.StorageClass); err != nil {
			return err
		}

		newGeneration, err := s.placementRepo.CompareAndSwap(
			ctx,
			placement.VersionID,
			*target.ExpectedGeneration,
			*target.ExpectedClass,
			target.StorageClass,
			target.BlobID,
		)
		if err != nil {
			return err
		}

		// placement still describes the previous blob, which becomes a source copy.
		source, err := s.copyRepo.InsertSource(ctx, migrationID, placement, sourceCleanupDelay)
		if err != nil {
			return err
		}
		if err := s.copyRepo.Promote(ctx, target.CopyID, verifiedMD5); err != nil {
			return err
		}

		result = &model.SwitchResult{
			VersionID:     placement.VersionID,
			NewGeneration: newGeneration,
			StorageClass:  target.StorageClass,
			BlobID:        target.BlobID,
			SourceCopy:    source,
		}
		return nil
	})
	if txErr != nil {
		return nil, txErr
	}

	return result, nil
}

// previousSwitch rebuilds the answer of a switch that already committed, so a
// retry after a lost response learns the outcome instead of failing.
func (s *placementService) previousSwitch(ctx context.Context, target *model.PlacementCopy) (*model.SwitchResult, error) {
	source, err := s.copyRepo.GetByMigration(ctx, target.MigrationID, model.CopyRoleSource)
	if err != nil {
		return nil, err
	}

	result := &model.SwitchResult{
		NewGeneration: *target.ExpectedGeneration + 1,
		StorageClass:  target.StorageClass,
		BlobID:        target.BlobID,
		SourceCopy:    source,
	}
	if target.VersionID != nil {
		result.VersionID = *target.VersionID
	}

	return result, nil
}
