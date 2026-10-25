package config

import (
	"errors"
	"os"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/config/env"
	"github.com/joho/godotenv"
)

type Config struct {
	HTTP     HTTPConfig
	Metadata MetadataClientConfig
	Storage  StorageClientConfig
	Logger   LoggerConfig
	Tracing  TracingConfig
	Metrics  MetricsConfig

	Kafka                 KafkaConfig
	Sarama                SaramaConfig
	AccessEvents          AccessEventsConfig
	ObjectReadProducer    ObjectReadProducerConfig
	ObjectWrittenProducer ObjectWrittenProducerConfig
}

// Load uses the process environment and, when present, a local .env file.
func Load(path ...string) (*Config, error) {
	if err := godotenv.Load(path...); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	httpConfig, err := env.NewHTTPConfig()
	if err != nil {
		return nil, err
	}
	metadataConfig, err := env.NewMetadataConfig()
	if err != nil {
		return nil, err
	}
	storageConfig, err := env.NewStorageConfig()
	if err != nil {
		return nil, err
	}
	loggerConfig, err := env.NewLoggerConfig()
	if err != nil {
		return nil, err
	}
	tracingConfig, err := env.NewTracingConfig()
	if err != nil {
		return nil, err
	}
	metricsConfig, err := env.NewMetricsConfig()
	if err != nil {
		return nil, err
	}
	kafkaConfig, err := env.NewKafkaConfig()
	if err != nil {
		return nil, err
	}
	saramaConfig, err := env.NewSaramaConfig()
	if err != nil {
		return nil, err
	}
	accessEventsConfig, err := env.NewAccessEventsConfig()
	if err != nil {
		return nil, err
	}
	objectReadProducerConfig, err := env.NewObjectReadProducerConfig()
	if err != nil {
		return nil, err
	}
	objectWrittenProducerConfig, err := env.NewObjectWrittenProducerConfig()
	if err != nil {
		return nil, err
	}
	return &Config{
		HTTP:     httpConfig,
		Metadata: metadataConfig,
		Storage:  storageConfig,
		Logger:   loggerConfig,
		Tracing:  tracingConfig,
		Metrics:  metricsConfig,

		Kafka:                 kafkaConfig,
		Sarama:                saramaConfig,
		AccessEvents:          accessEventsConfig,
		ObjectReadProducer:    objectReadProducerConfig,
		ObjectWrittenProducer: objectWrittenProducerConfig,
	}, nil
}
