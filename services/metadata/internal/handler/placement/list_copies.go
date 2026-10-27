package placement

import (
	"context"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func (h *handler) ListMigrationCopies(ctx context.Context, req *metadatav1.ListMigrationCopiesRequest) (*metadatav1.ListMigrationCopiesResponse, error) {
	copies, err := h.placementService.ListMigrationCopies(ctx, req.GetMigrationId())
	if err != nil {
		return nil, err
	}

	return &metadatav1.ListMigrationCopiesResponse{Copies: copiesToProto(copies)}, nil
}

func (h *handler) ListDueCleanups(ctx context.Context, req *metadatav1.ListDueCleanupsRequest) (*metadatav1.ListDueCleanupsResponse, error) {
	copies, err := h.placementService.ListDueCleanups(ctx, req.GetLimit())
	if err != nil {
		return nil, err
	}

	return &metadatav1.ListDueCleanupsResponse{Copies: copiesToProto(copies)}, nil
}
