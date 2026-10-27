package placement

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func (h *handler) ReserveCopy(ctx context.Context, req *metadatav1.ReserveCopyRequest) (*metadatav1.ReserveCopyResponse, error) {
	c, err := h.placementService.ReserveCopy(
		ctx,
		req.GetMigrationId(),
		req.GetVersionId(),
		req.GetExpectedGeneration(),
		model.StorageClass(req.GetSourceClass()),
		model.StorageClass(req.GetTargetClass()),
	)
	if err != nil {
		return nil, err
	}

	return &metadatav1.ReserveCopyResponse{Copy: copyToProto(c)}, nil
}
