package app

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/config"
	converterKafka "github.com/alesplll/opens3-rebac/services/sppr/internal/converter/kafka"
	decoderKafka "github.com/alesplll/opens3-rebac/services/sppr/internal/converter/kafka/decoder"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/observability"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	eventRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/event"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	accessConsumerService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/consumer/access"
	ingestorService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/ingestor"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/pg"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/transaction"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/closer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	kafkaConsumer "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/consumer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	kafkaMiddleware "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/middleware/kafka"
	"go.uber.org/zap"
)

// Dependencies are resolved during startup on one goroutine, then immutable.
type serviceProvider struct {
	cfg    *config.Config
	closer *closer.Closer

	pgClient        db.Client
	txManager       db.TxManager
	eventRepository repository.EventRepository
	ingestor        service.Ingestor

	objectReadConsumerGroup sarama.ConsumerGroup
	objectReadConsumer      kafka.Consumer
	objectReadDecoder       converterKafka.ObjectReadDecoder

	objectWrittenConsumerGroup sarama.ConsumerGroup
	objectWrittenConsumer      kafka.Consumer
	objectWrittenDecoder       converterKafka.ObjectWrittenDecoder

	accessConsumerService service.AccessConsumerService
}

func newServiceProvider(cfg *config.Config, closer *closer.Closer) *serviceProvider {
	return &serviceProvider{cfg: cfg, closer: closer}
}

func (s *serviceProvider) PGClient(ctx context.Context) (db.Client, error) {
	if s.pgClient == nil {
		ctx, cancel := context.WithTimeout(ctx, s.cfg.PG.Timeout())
		defer cancel()
		client, err := pg.NewPGClient(ctx, logger.Logger(), s.cfg.PG)
		if err != nil {
			return nil, fmt.Errorf("connect PostgreSQL: %w", err)
		}
		s.closer.AddNamed("PGClient", func(context.Context) error { return client.Close() })
		if err := client.DB().Ping(ctx); err != nil {
			return nil, err
		}
		// Fail startup if migrations were forgotten, without consuming any offsets.
		var schema bool
		err = client.DB().QueryRowContext(ctx, db.Query{Name: "schema:check", QueryRaw: `SELECT
			to_regclass('ingestion_streams') IS NOT NULL AND to_regclass('access_events') IS NOT NULL
			AND to_regclass('ingestion_gaps') IS NOT NULL AND to_regclass('read_aggregates') IS NOT NULL`}).Scan(&schema)
		if err != nil || !schema {
			return nil, fmt.Errorf("ingestion schema unavailable; apply migrations (query error: %v)", err)
		}
		s.pgClient = client
	}
	return s.pgClient, nil
}

func (s *serviceProvider) EventRepository(ctx context.Context) (repository.EventRepository, error) {
	if s.eventRepository == nil {
		client, err := s.PGClient(ctx)
		if err != nil {
			return nil, err
		}
		s.eventRepository = eventRepository.NewRepository(client)
	}
	return s.eventRepository, nil
}

func (s *serviceProvider) TxManager(ctx context.Context) (db.TxManager, error) {
	if s.txManager == nil {
		client, err := s.PGClient(ctx)
		if err != nil {
			return nil, err
		}
		s.txManager = transaction.NewTransactionManager(client.DB())
	}
	return s.txManager, nil
}

func (s *serviceProvider) Ingestor(ctx context.Context) (service.Ingestor, error) {
	if s.ingestor == nil {
		repo, err := s.EventRepository(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := s.TxManager(ctx)
		if err != nil {
			return nil, err
		}
		s.ingestor = ingestorService.NewService(repo, tx)
	}
	return s.ingestor, nil
}

func (s *serviceProvider) ObjectReadConsumerGroup() (sarama.ConsumerGroup, error) {
	if s.objectReadConsumerGroup == nil {
		group, err := s.newConsumerGroup("object-read")
		if err != nil {
			return nil, err
		}
		s.objectReadConsumerGroup = group
	}
	return s.objectReadConsumerGroup, nil
}

func (s *serviceProvider) ObjectWrittenConsumerGroup() (sarama.ConsumerGroup, error) {
	if s.objectWrittenConsumerGroup == nil {
		group, err := s.newConsumerGroup("object-written")
		if err != nil {
			return nil, err
		}
		s.objectWrittenConsumerGroup = group
	}
	return s.objectWrittenConsumerGroup, nil
}

func (s *serviceProvider) newConsumerGroup(name string) (sarama.ConsumerGroup, error) {
	group, err := sarama.NewConsumerGroup(
		s.cfg.Kafka.Brokers(),
		s.cfg.Sarama.GroupID(),
		s.cfg.Sarama.Config(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka %s consumer group: %w", name, err)
	}
	// Consumer.Return.Errors is on, so the channel must be drained; it closes with the group.
	go func() {
		for err := range group.Errors() {
			logger.Error(context.Background(), "Kafka consumer group error", zap.String("group", name), zap.Error(err))
		}
	}()
	s.closer.AddNamed("Kafka "+name+" consumer group", func(context.Context) error {
		return group.Close()
	})
	return group, nil
}

func (s *serviceProvider) ObjectReadConsumer() (kafka.Consumer, error) {
	if s.objectReadConsumer == nil {
		group, err := s.ObjectReadConsumerGroup()
		if err != nil {
			return nil, err
		}
		s.objectReadConsumer = kafkaConsumer.NewConsumer(
			group,
			[]string{s.cfg.ObjectReadConsumer.Topic()},
			logger.Logger(),
			s.consumerMiddlewares()...,
		)
	}
	return s.objectReadConsumer, nil
}

func (s *serviceProvider) ObjectWrittenConsumer() (kafka.Consumer, error) {
	if s.objectWrittenConsumer == nil {
		group, err := s.ObjectWrittenConsumerGroup()
		if err != nil {
			return nil, err
		}
		s.objectWrittenConsumer = kafkaConsumer.NewConsumer(
			group,
			[]string{s.cfg.ObjectWrittenConsumer.Topic()},
			logger.Logger(),
			s.consumerMiddlewares()...,
		)
	}
	return s.objectWrittenConsumer, nil
}

// Logging comes first so that retries of one message are logged once.
func (s *serviceProvider) consumerMiddlewares() []kafkaConsumer.Middleware {
	return []kafkaConsumer.Middleware{
		kafkaMiddleware.Logging(logger.Logger()),
		kafkaMiddleware.Retry(s.cfg.Retry.Interval()),
	}
}

func (s *serviceProvider) ObjectReadDecoder() converterKafka.ObjectReadDecoder {
	if s.objectReadDecoder == nil {
		s.objectReadDecoder = decoderKafka.NewObjectReadDecoder()
	}
	return s.objectReadDecoder
}

func (s *serviceProvider) ObjectWrittenDecoder() converterKafka.ObjectWrittenDecoder {
	if s.objectWrittenDecoder == nil {
		s.objectWrittenDecoder = decoderKafka.NewObjectWrittenDecoder()
	}
	return s.objectWrittenDecoder
}

func (s *serviceProvider) AccessConsumerService(ctx context.Context) (service.AccessConsumerService, error) {
	if s.accessConsumerService == nil {
		ingestor, err := s.Ingestor(ctx)
		if err != nil {
			return nil, err
		}
		metrics, err := observability.NewMetrics()
		if err != nil {
			return nil, err
		}
		objectReadConsumer, err := s.ObjectReadConsumer()
		if err != nil {
			return nil, err
		}
		objectWrittenConsumer, err := s.ObjectWrittenConsumer()
		if err != nil {
			return nil, err
		}
		s.accessConsumerService = accessConsumerService.NewService(
			objectReadConsumer,
			s.ObjectReadDecoder(),
			objectWrittenConsumer,
			s.ObjectWrittenDecoder(),
			ingestor,
			metrics,
		)
	}
	return s.accessConsumerService, nil
}
