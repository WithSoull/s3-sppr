package placement

import (
	"context"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func (h *handler) ListStorageClasses(ctx context.Context, _ *metadatav1.ListStorageClassesRequest) (*metadatav1.ListStorageClassesResponse, error) {
	usage, err := h.placementService.ListStorageClasses(ctx)
	if err != nil {
		return nil, err
	}

	classes := make([]*metadatav1.StorageClassInfo, 0, len(usage))
	for _, u := range usage {
		classes = append(classes, storageClassToProto(u))
	}

	return &metadatav1.ListStorageClassesResponse{StorageClasses: classes}, nil
}

func (h *handler) ListPlacements(ctx context.Context, req *metadatav1.ListPlacementsRequest) (*metadatav1.ListPlacementsResponse, error) {
	placements, nextToken, err := h.placementService.ListPlacements(ctx, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}

	res := &metadatav1.ListPlacementsResponse{
		Placements:    make([]*metadatav1.PlacementInfo, 0, len(placements)),
		NextPageToken: nextToken,
	}
	for _, p := range placements {
		res.Placements = append(res.Placements, placementToProto(p))
	}

	return res, nil
}
