package metadata

import (
	"context"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func (h *handler) ChooseWriteClass(ctx context.Context, req *metadatav1.ChooseWriteClassRequest) (*metadatav1.ChooseWriteClassResponse, error) {
	decision, err := h.objectService.ChooseWriteClass(ctx, req.GetBucketName(), req.GetKey(), req.SizeBytes)
	if err != nil {
		return nil, err
	}

	return &metadatav1.ChooseWriteClassResponse{
		StorageClass: string(decision.StorageClass),
		Reason:       string(decision.Reason),
	}, nil
}
