package placement

import (
	"github.com/alesplll/opens3-rebac/services/metadata/internal/service"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

type handler struct {
	metadatav1.UnimplementedPlacementServiceServer
	placementService service.PlacementService
}

func NewHandler(placementService service.PlacementService) metadatav1.PlacementServiceServer {
	return &handler{placementService: placementService}
}
