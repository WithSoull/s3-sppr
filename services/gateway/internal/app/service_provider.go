package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/client"
	metadataclient "github.com/alesplll/opens3-rebac/services/gateway/internal/client/metadata"
	storageclient "github.com/alesplll/opens3-rebac/services/gateway/internal/client/storage"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/config"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
	objectservice "github.com/alesplll/opens3-rebac/services/gateway/internal/service/object"
	accessproducer "github.com/alesplll/opens3-rebac/services/gateway/internal/service/producer/access"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/closer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	kafkaproducer "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/producer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/tracing"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type serviceProvider struct {
	config *config.Config
	closer *closer.Closer

	metadataConn   *grpc.ClientConn
	storageConns   []*grpc.ClientConn
	metadataClient client.MetadataClient
	storageClients map[model.StorageClass]client.StorageClient

	asyncProducer         sarama.AsyncProducer
	objectReadProducer    kafka.Producer
	objectWrittenProducer kafka.Producer
	accessProducerService service.AccessProducerService

	objectService service.ObjectService
	objectHandler http.Handler
	closeOnce     sync.Once
	closeErr      error
}

func newServiceProvider(cfg *config.Config, closer *closer.Closer) *serviceProvider {
	return &serviceProvider{config: cfg, closer: closer}
}

func (s *serviceProvider) MetadataClient() (client.MetadataClient, error) {
	if s.metadataClient == nil {
		conn, err := grpc.NewClient(
			s.config.Metadata.Address(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(tracing.UnaryClientInterceptor("gateway.metadata")),
		)
		if err != nil {
			return nil, fmt.Errorf("create Metadata client: %w", err)
		}
		s.metadataConn = conn
		s.metadataClient = metadataclient.NewClient(metadatav1.NewMetadataServiceClient(conn), s.config.Metadata.Timeout())
	}
	return s.metadataClient, nil
}

// StorageClients connects to one Storage instance per storage class.
func (s *serviceProvider) StorageClients() (map[model.StorageClass]client.StorageClient, error) {
	if s.storageClients == nil {
		addresses := map[model.StorageClass]string{
			model.StorageClassHot:  s.config.Storage.HotAddress(),
			model.StorageClassCold: s.config.Storage.ColdAddress(),
		}
		clients := make(map[model.StorageClass]client.StorageClient, len(addresses))
		for class, address := range addresses {
			conn, err := grpc.NewClient(
				address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStreamInterceptor(tracing.StreamClientInterceptor("gateway.storage")),
			)
			if err != nil {
				return nil, fmt.Errorf("create %s Storage client: %w", class, err)
			}
			s.storageConns = append(s.storageConns, conn)
			clients[class] = storageclient.NewClient(
				storagev1.NewDataStorageServiceClient(conn),
				s.config.Storage.RetrieveChunkSizeBytes(),
			)
		}
		s.storageClients = clients
	}
	return s.storageClients, nil
}

// AsyncProducer is shared by both topics. Its buffer is flushed by Close, which
// the closer bounds with the shutdown timeout.
func (s *serviceProvider) AsyncProducer() (sarama.AsyncProducer, error) {
	if s.asyncProducer == nil {
		deliveryErrors, err := otel.Meter("gateway.access").Int64Counter("gateway_kafka_delivery_errors_total")
		if err != nil {
			return nil, err
		}
		p, err := sarama.NewAsyncProducer(s.config.Kafka.Brokers(), s.config.Sarama.Config())
		if err != nil {
			return nil, fmt.Errorf("create Kafka async producer: %w", err)
		}
		go kafkaproducer.LogResults(p, logger.Logger(), func(topic string) {
			deliveryErrors.Add(context.Background(), 1, metric.WithAttributes(attribute.String("topic", topic)))
		})
		s.closer.AddNamed("Kafka async producer", func(context.Context) error { return p.Close() })
		s.asyncProducer = p
	}
	return s.asyncProducer, nil
}

func (s *serviceProvider) ObjectReadProducer() (kafka.Producer, error) {
	if s.objectReadProducer == nil {
		p, err := s.AsyncProducer()
		if err != nil {
			return nil, err
		}
		s.objectReadProducer = kafkaproducer.NewAsyncProducer(p, s.config.ObjectReadProducer.Topic())
	}
	return s.objectReadProducer, nil
}

func (s *serviceProvider) ObjectWrittenProducer() (kafka.Producer, error) {
	if s.objectWrittenProducer == nil {
		p, err := s.AsyncProducer()
		if err != nil {
			return nil, err
		}
		s.objectWrittenProducer = kafkaproducer.NewAsyncProducer(p, s.config.ObjectWrittenProducer.Topic())
	}
	return s.objectWrittenProducer, nil
}

func (s *serviceProvider) AccessProducerService() (service.AccessProducerService, error) {
	if s.accessProducerService == nil {
		if !s.config.AccessEvents.Enabled() {
			s.accessProducerService = accessproducer.NewNoopService()
			return s.accessProducerService, nil
		}
		objectReadProducer, err := s.ObjectReadProducer()
		if err != nil {
			return nil, err
		}
		objectWrittenProducer, err := s.ObjectWrittenProducer()
		if err != nil {
			return nil, err
		}
		dropped, err := otel.Meter("gateway.access").Int64Counter("gateway_access_events_dropped_total")
		if err != nil {
			return nil, err
		}
		access := accessproducer.NewService(
			s.config.AccessEvents.GatewayID(),
			objectReadProducer,
			objectWrittenProducer,
			dropped,
		)
		access.Start(s.config.AccessEvents.HeartbeatInterval())
		s.accessProducerService = access
	}
	return s.accessProducerService, nil
}

func (s *serviceProvider) ObjectService() (service.ObjectService, error) {
	if s.objectService == nil {
		metadata, err := s.MetadataClient()
		if err != nil {
			return nil, err
		}
		storage, err := s.StorageClients()
		if err != nil {
			return nil, err
		}
		access, err := s.AccessProducerService()
		if err != nil {
			return nil, err
		}
		s.objectService = objectservice.NewService(
			metadata,
			storage,
			access,
		)
	}
	return s.objectService, nil
}

func (s *serviceProvider) ObjectHandler() (http.Handler, error) {
	if s.objectHandler == nil {
		objects, err := s.ObjectService()
		if err != nil {
			return nil, err
		}
		s.objectHandler = httpapi.NewHandler(objects, s.config.HTTP.MaxReadDuration())
	}
	return s.objectHandler, nil
}

func (s *serviceProvider) Close() error {
	s.closeOnce.Do(func() {
		var errs []error
		for _, conn := range s.storageConns {
			if err := conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close Storage client %s: %w", conn.Target(), err))
			}
		}
		if s.metadataConn != nil {
			if err := s.metadataConn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close Metadata client: %w", err))
			}
		}
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}
