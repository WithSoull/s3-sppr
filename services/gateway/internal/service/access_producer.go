package service

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
)

// AccessProducerService publishes successful object operations for SPPR. The
// events are telemetry, so publishing errors never reach the S3 operation.
type AccessProducerService interface {
	ProduceObjectRead(ctx context.Context, event model.ObjectReadEvent)
	ProduceObjectWritten(ctx context.Context, event model.ObjectWrittenEvent)
}
