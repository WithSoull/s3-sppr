package object

import (
	"github.com/alesplll/opens3-rebac/services/gateway/internal/client"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
)

type objectService struct {
	metadata client.MetadataClient
	storage  client.StorageClient
	access   service.AccessProducerService
}

func NewService(
	metadataClient client.MetadataClient,
	storageClient client.StorageClient,
	accessProducerService service.AccessProducerService,
) service.ObjectService {
	return &objectService{
		metadata: metadataClient,
		storage:  storageClient,
		access:   accessProducerService,
	}
}
