package object

import (
	"context"
	"errors"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
)

// ChooseWriteClass keeps a key in HOT across overwrites: the administrator
// already approved HOT for it. New keys start in COLD so uploads never fill HOT
// outside an approved plan. The space check does not reserve anything.
func (s *objectService) ChooseWriteClass(ctx context.Context, bucketName, key string, sizeBytes *int64) (*model.WriteClassDecision, error) {
	if err := validate.Validate(ctx,
		func(ctx context.Context) error {
			if bucketName == "" {
				return validate.NewValidationErrors("bucket_name is required")
			}
			return nil
		},
		func(ctx context.Context) error {
			if key == "" {
				return validate.NewValidationErrors("key is required")
			}
			return nil
		},
		func(ctx context.Context) error {
			if sizeBytes != nil && *sizeBytes < 0 {
				return validate.NewValidationErrors("size_bytes must not be negative")
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	current, err := s.repo.GetMeta(ctx, bucketName, key, "")
	if errors.Is(err, domainerrors.ErrObjectNotFound) {
		return cold(model.WriteClassReasonNewKey), nil
	}
	if err != nil {
		return nil, err
	}
	if current.StorageClass != model.StorageClassHot {
		return cold(model.WriteClassReasonPreviousCold), nil
	}
	if sizeBytes == nil {
		return cold(model.WriteClassReasonSizeUnknown), nil
	}

	usage, err := s.placementRepo.GetUsage(ctx, model.StorageClassHot)
	if err != nil {
		return nil, err
	}
	if usage.FreeBytes() < *sizeBytes {
		return cold(model.WriteClassReasonHotFull), nil
	}

	return &model.WriteClassDecision{StorageClass: model.StorageClassHot, Reason: model.WriteClassReasonInherited}, nil
}

func cold(reason model.WriteClassReason) *model.WriteClassDecision {
	return &model.WriteClassDecision{StorageClass: model.StorageClassCold, Reason: reason}
}
