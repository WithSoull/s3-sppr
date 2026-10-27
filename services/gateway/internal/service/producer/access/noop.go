package access

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
)

type noopService struct{}

// NewNoopService serves ACCESS_EVENTS_ENABLED=false: no Kafka client is created.
func NewNoopService() service.AccessProducerService {
	return noopService{}
}

func (noopService) ProduceObjectRead(context.Context, model.ObjectReadEvent)       {}
func (noopService) ProduceObjectWritten(context.Context, model.ObjectWrittenEvent) {}
