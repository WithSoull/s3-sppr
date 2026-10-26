package placement

import (
	"context"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func (h *handler) AbortCopy(ctx context.Context, req *metadatav1.AbortCopyRequest) (*metadatav1.AbortCopyResponse, error) {
	c, err := h.placementService.AbortCopy(ctx, req.GetMigrationId(), req.GetReason())
	if err != nil {
		return nil, err
	}

	return &metadatav1.AbortCopyResponse{Copy: copyToProto(c)}, nil
}
