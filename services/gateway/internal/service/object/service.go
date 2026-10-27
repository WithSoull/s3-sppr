package object

import (
	"fmt"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/client"
	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
)

type objectService struct {
	metadata client.MetadataClient
	storage  map[model.StorageClass]client.StorageClient
	access   service.AccessProducerService
}

func NewService(
	metadataClient client.MetadataClient,
	storageClients map[model.StorageClass]client.StorageClient,
	accessProducerService service.AccessProducerService,
) service.ObjectService {
	return &objectService{
		metadata: metadataClient,
		storage:  storageClients,
		access:   accessProducerService,
	}
}

func (s *objectService) storageFor(class model.StorageClass) (client.StorageClient, error) {
	storage, ok := s.storage[class]
	if !ok {
		return nil, fmt.Errorf("%w: %q", domainerrors.ErrUnknownStorageClass, class)
	}
	return storage, nil
}
