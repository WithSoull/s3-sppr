package access

import (
	"context"
	"time"

	kafkaConverter "github.com/alesplll/opens3-rebac/services/sppr/internal/converter/kafka"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"golang.org/x/sync/errgroup"
)

type Observer interface {
	Observe(ctx context.Context, outcome string, missing int64, elapsed time.Duration)
}

type accessConsumerService struct {
	objectReadConsumer kafka.Consumer
	objectReadDecoder  kafkaConverter.ObjectReadDecoder

	objectWrittenConsumer kafka.Consumer
	objectWrittenDecoder  kafkaConverter.ObjectWrittenDecoder

	ingestor service.Ingestor
	observer Observer
}

func NewService(
	objectReadConsumer kafka.Consumer,
	objectReadDecoder kafkaConverter.ObjectReadDecoder,

	objectWrittenConsumer kafka.Consumer,
	objectWrittenDecoder kafkaConverter.ObjectWrittenDecoder,

	ingestor service.Ingestor,
	observer Observer,
) *accessConsumerService {
	return &accessConsumerService{
		objectReadConsumer: objectReadConsumer,
		objectReadDecoder:  objectReadDecoder,

		objectWrittenConsumer: objectWrittenConsumer,
		objectWrittenDecoder:  objectWrittenDecoder,

		ingestor: ingestor,
		observer: observer,
	}
}

func (s *accessConsumerService) RunConsumer(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.objectReadConsumer.Consume(ctx, s.ObjectReadHandler)
	})

	g.Go(func() error {
		return s.objectWrittenConsumer.Consume(ctx, s.ObjectWrittenHandler)
	})

	return g.Wait()
}
