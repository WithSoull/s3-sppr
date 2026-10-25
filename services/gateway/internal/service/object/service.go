package object

import (
	"github.com/alesplll/opens3-rebac/services/gateway/internal/client"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
)

type objectService struct {
	metadata client.MetadataClient
	storage  client.StorageClient
}

func NewService(
	metadataClient client.MetadataClient,
	storageClient client.StorageClient,
) service.ObjectService {
	return &objectService{
		metadata: metadataClient,
		storage:  storageClient,
	}
}
