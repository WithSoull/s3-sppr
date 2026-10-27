package placement

import (
	"context"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
)

func (s *placementService) CompleteCleanup(ctx context.Context, copyID string, deleted bool, errorMessage string) (*model.PlacementCopy, error) {
	if err := validate.Validate(ctx,
		uuidField("copy_id", copyID),
		func(context.Context) error {
			if !deleted && errorMessage == "" {
				return validate.NewValidationErrors("error_message is required when deleted is false")
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	var completed *model.PlacementCopy
	txErr := s.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		c, err := s.copyRepo.LockByID(ctx, copyID)
		if err != nil {
			return err
		}

		switch {
		case c.State == model.CopyStateDeleted && deleted:
			completed = c
			return nil
		case c.State != model.CopyStateCleanupPending && c.State != model.CopyStateCleanupFailed:
			return domainerrors.ErrInvalidCopyState
		case deleted:
			completed, err = s.copyRepo.MarkDeleted(ctx, copyID)
			return err
		default:
			completed, err = s.copyRepo.MarkCleanupFailed(ctx, copyID, errorMessage)
			return err
		}
	})
	if txErr != nil {
		return nil, txErr
	}

	return completed, nil
}
