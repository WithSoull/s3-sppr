package placement

import (
	"context"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func (h *handler) CompleteCleanup(ctx context.Context, req *metadatav1.CompleteCleanupRequest) (*metadatav1.CompleteCleanupResponse, error) {
	c, err := h.placementService.CompleteCleanup(ctx, req.GetCopyId(), req.GetDeleted(), req.GetErrorMessage())
	if err != nil {
		return nil, err
	}

	return &metadatav1.CompleteCleanupResponse{Copy: copyToProto(c)}, nil
}
