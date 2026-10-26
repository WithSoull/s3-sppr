package placement

import (
	"context"
	"time"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func (h *handler) SwitchPlacement(ctx context.Context, req *metadatav1.SwitchPlacementRequest) (*metadatav1.SwitchPlacementResponse, error) {
	res, err := h.placementService.SwitchPlacement(
		ctx,
		req.GetMigrationId(),
		req.GetVerifiedSizeBytes(),
		req.GetVerifiedMd5(),
		time.Duration(req.GetSourceCleanupDelayMs())*time.Millisecond,
	)
	if err != nil {
		return nil, err
	}

	return &metadatav1.SwitchPlacementResponse{
		VersionId:     res.VersionID,
		NewGeneration: res.NewGeneration,
		StorageClass:  string(res.StorageClass),
		BlobId:        res.BlobID,
		SourceCopy:    copyToProto(res.SourceCopy),
	}, nil
}
