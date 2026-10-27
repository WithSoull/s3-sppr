package placement

import (
	"context"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
)

func (s *placementService) AbortCopy(ctx context.Context, migrationID, reason string) (*model.PlacementCopy, error) {
	if err := validate.Validate(ctx, uuidField("migration_id", migrationID)); err != nil {
		return nil, err
	}

	var aborted *model.PlacementCopy
	txErr := s.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		target, err := s.copyRepo.LockByMigration(ctx, migrationID, model.CopyRoleTarget)
		if err != nil {
			return err
		}

		switch target.State {
		case model.CopyStateReserved:
			// The target never served reads, so its blob may be deleted at once.
			aborted, err = s.copyRepo.ScheduleCleanup(ctx, target.CopyID, 0, reason)
			return err
		case model.CopyStateCleanupPending, model.CopyStateCleanupFailed, model.CopyStateDeleted:
			aborted = target
			return nil
		default:
			return domainerrors.ErrInvalidCopyState
		}
	})
	if txErr != nil {
		return nil, txErr
	}

	return aborted, nil
}
