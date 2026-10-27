package placement

import (
	"context"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func (h *handler) GetPlacement(ctx context.Context, req *metadatav1.GetPlacementRequest) (*metadatav1.GetPlacementResponse, error) {
	p, err := h.placementService.GetPlacement(ctx, req.GetVersionId())
	if err != nil {
		return nil, err
	}

	return &metadatav1.GetPlacementResponse{Placement: placementToProto(p)}, nil
}
