package placement

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
)

const maxPlacementPageSize = 1000

func (s *placementService) ListStorageClasses(ctx context.Context) ([]*model.StorageClassUsage, error) {
	return s.placementRepo.ListUsage(ctx)
}

func (s *placementService) ListPlacements(ctx context.Context, pageSize int32, pageToken string) ([]*model.Placement, string, error) {
	if pageToken != "" {
		if err := validate.Validate(ctx, uuidField("page_token", pageToken)); err != nil {
			return nil, "", err
		}
	}
	if pageSize <= 0 || pageSize > maxPlacementPageSize {
		pageSize = maxPlacementPageSize
	}

	// One extra row tells whether another page exists, so the last page never
	// returns a token that leads to an empty page.
	placements, err := s.placementRepo.ListCurrent(ctx, pageToken, pageSize+1)
	if err != nil {
		return nil, "", err
	}
	if len(placements) <= int(pageSize) {
		return placements, "", nil
	}

	placements = placements[:pageSize]
	return placements, placements[pageSize-1].VersionID, nil
}
