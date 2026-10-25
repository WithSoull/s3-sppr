package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	accesspublisher "github.com/alesplll/opens3-rebac/services/gateway/internal/publisher/access"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/producer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/google/uuid"
)

func (s *serviceProvider) AccessPublisher() (httpapi.AccessRecorder, error) {
	if s.config.Access == nil || !s.config.Access.Enabled() {
		return nil, nil
	}
	if s.accessPublisher != nil {
		return s.accessPublisher, nil
	}
	settings := s.config.Access
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.ClientID = "gateway-access-" + settings.GatewayID()
	cfg.Net.MaxOpenRequests = 1
	cfg.Net.DialTimeout = settings.SendTimeout()
	cfg.Net.ReadTimeout = settings.SendTimeout()
	cfg.Net.WriteTimeout = settings.SendTimeout()
	cfg.Metadata.Full = false
	cfg.Metadata.Timeout = settings.SendTimeout()
	cfg.Metadata.Retry.Max = 1
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Timeout = settings.SendTimeout()
	cfg.Producer.Retry.Max = 1
	client, err := sarama.NewClient(settings.Brokers(), cfg)
	if err != nil {
		return nil, fmt.Errorf("connect access event Kafka: %w", err)
	}
	partitions, err := client.Partitions(settings.AccessTopic())
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("get access topic partitions: %w", err)
	}
	if len(partitions) == 0 {
		_ = client.Close()
		return nil, fmt.Errorf("access topic has no partitions")
	}
	streamID := uuid.NewString()
	index, err := sarama.NewHashPartitioner(settings.AccessTopic()).Partition(&sarama.ProducerMessage{Key: sarama.StringEncoder(settings.GatewayID() + "/" + streamID)}, int32(len(partitions)))
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	// Hash once: expanding the topic must not move an existing ingestion stream.
	partition := partitions[index]
	cfg.Producer.Partitioner = func(string) sarama.Partitioner { return pinnedPartitioner{partition: partition} }
	transport, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	adapter := producer.NewAsyncProducer(transport, settings.AccessTopic())
	publisher, err := accesspublisher.NewPublisher(settings, adapter, logger.Logger(), streamID)
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), settings.ShutdownTimeout())
		defer cancel()
		return nil, errors.Join(err, adapter.Close(ctx), client.Close())
	}
	s.accessPublisher = publisher
	s.accessTransport = adapter
	s.accessClient = client
	return publisher, nil
}

func (s *serviceProvider) CloseAccess(ctx context.Context) error {
	if s.accessPublisher == nil {
		return nil
	}
	s.accessCloseOnce.Do(func() {
		drainErr := s.accessPublisher.Close(ctx)
		// Drain may exhaust its deadline. The transport still needs time to finish
		// in-flight acknowledgements and join its completion router.
		cleanup, cancel := context.WithTimeout(context.Background(), s.config.Access.ShutdownTimeout())
		defer cancel()
		transportErr := s.accessTransport.Close(cleanup)
		clientErr := s.accessClient.Close()
		s.accessCloseErr = errors.Join(drainErr, transportErr, clientErr)
	})
	return s.accessCloseErr
}

type pinnedPartitioner struct{ partition int32 }

func (p pinnedPartitioner) Partition(_ *sarama.ProducerMessage, count int32) (int32, error) {
	if p.partition >= count {
		return 0, fmt.Errorf("access event partition %d no longer exists", p.partition)
	}
	return p.partition, nil
}
func (p pinnedPartitioner) RequiresConsistency() bool { return true }
