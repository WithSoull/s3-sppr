package app

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/config"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/observability"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	eventRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/event"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	ingestorService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/ingestor"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/worker/ingestion"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/pg"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db/transaction"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.opentelemetry.io/otel"
)

// Dependencies are resolved during startup on one goroutine, then immutable.
type serviceProvider struct {
	cfg             *config.Config
	pgClient        db.Client
	txManager       db.TxManager
	group           sarama.ConsumerGroup
	eventRepository repository.EventRepository
	ingestor        service.Ingestor
	accessHandler   *access.Handler
	worker          *ingestion.Worker
}

func newServiceProvider(cfg *config.Config) *serviceProvider { return &serviceProvider{cfg: cfg} }

func (s *serviceProvider) PGClient(ctx context.Context) (db.Client, error) {
	if s.pgClient == nil {
		ctx, cancel := context.WithTimeout(ctx, s.cfg.PG.Timeout())
		defer cancel()
		client, err := pg.NewPGClient(ctx, logger.Logger(), s.cfg.PG)
		if err != nil {
			return nil, fmt.Errorf("connect PostgreSQL: %w", err)
		}
		if err := client.DB().Ping(ctx); err != nil {
			_ = client.Close()
			return nil, err
		}
		// Fail startup if migrations were forgotten, without consuming any offsets.
		var schema bool
		err = client.DB().QueryRowContext(ctx, db.Query{Name: "schema:check", QueryRaw: `SELECT
			to_regclass('ingestion_streams') IS NOT NULL AND to_regclass('access_events') IS NOT NULL
			AND to_regclass('ingestion_gaps') IS NOT NULL AND to_regclass('read_aggregates') IS NOT NULL`}).Scan(&schema)
		if err != nil || !schema {
			_ = client.Close()
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

func (s *serviceProvider) AccessHandler(ctx context.Context) (*access.Handler, error) {
	if s.accessHandler == nil {
		ingestor, err := s.Ingestor(ctx)
		if err != nil {
			return nil, err
		}
		metrics, err := observability.NewMetrics()
		if err != nil {
			return nil, err
		}
		s.accessHandler = access.NewHandler(ingestor, metrics, otel.Tracer("sppr/ingestion"), s.cfg.Kafka.MaxEventBytes())
	}
	return s.accessHandler, nil
}

func (s *serviceProvider) ConsumerGroup() (sarama.ConsumerGroup, error) {
	if s.group == nil {
		cfg := sarama.NewConfig()
		cfg.Version = sarama.V2_8_0_0
		cfg.ClientID = "sppr-ingestor"
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
		cfg.Consumer.Return.Errors = true
		group, err := sarama.NewConsumerGroup(s.cfg.Kafka.Brokers(), s.cfg.Kafka.GroupID(), cfg)
		if err != nil {
			return nil, fmt.Errorf("create Kafka consumer group: %w", err)
		}
		s.group = group
	}
	return s.group, nil
}

func (s *serviceProvider) Worker(ctx context.Context) (*ingestion.Worker, error) {
	if s.worker == nil {
		handler, err := s.AccessHandler(ctx)
		if err != nil {
			return nil, err
		}
		group, err := s.ConsumerGroup()
		if err != nil {
			return nil, err
		}
		s.worker = ingestion.NewWorker(group, handler.Handle, s.cfg.Kafka, logger.Logger())
	}
	return s.worker, nil
}
