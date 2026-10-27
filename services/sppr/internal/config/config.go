package config

import (
	"fmt"
	"os"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/config/env"
	"github.com/joho/godotenv"
)

type Config struct {
	Logger  LoggerConfig
	PG      PGConfig
	Tracing TracingConfig
	Metrics MetricsConfig
	HTTP    HTTPConfig

	Kafka                 KafkaConfig
	Sarama                SaramaConfig
	ObjectReadConsumer    ObjectReadConsumerConfig
	ObjectWrittenConsumer ObjectWrittenConsumerConfig
	Retry                 RetryConfig

	Migration MigrationConfig
	Upstream  UpstreamConfig
}

func Load(path string) (*Config, error) {
	if path != "" {
		if err := godotenv.Load(path); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	loggerCfg, err := env.NewLoggerConfig()
	if err != nil {
		return nil, err
	}
	pgCfg, err := env.NewPGConfig()
	if err != nil {
		return nil, err
	}
	tracingCfg, err := env.NewTracingConfig()
	if err != nil {
		return nil, err
	}
	metricsCfg, err := env.NewMetricsConfig()
	if err != nil {
		return nil, err
	}
	httpCfg, err := env.NewHTTPConfig()
	if err != nil {
		return nil, err
	}
	kafkaCfg, err := env.NewKafkaConfig()
	if err != nil {
		return nil, err
	}
	saramaCfg, err := env.NewSaramaConfig()
	if err != nil {
		return nil, err
	}
	objectReadConsumerCfg, err := env.NewObjectReadConsumerConfig()
	if err != nil {
		return nil, err
	}
	objectWrittenConsumerCfg, err := env.NewObjectWrittenConsumerConfig()
	if err != nil {
		return nil, err
	}
	retryCfg, err := env.NewRetryConfig()
	if err != nil {
		return nil, err
	}
	migrationCfg, err := env.NewMigrationConfig()
	if err != nil {
		return nil, err
	}
	upstreamCfg, err := env.NewUpstreamConfig()
	if err != nil {
		return nil, err
	}
	if pgCfg.Timeout() <= 0 || metricsCfg.PushTimeout() <= 0 {
		return nil, fmt.Errorf("PG_TIMEOUT and OTEL_METRICS_PUSH_TIMEOUT must be positive")
	}
	return &Config{
		Logger:  loggerCfg,
		PG:      pgCfg,
		Tracing: tracingCfg,
		Metrics: metricsCfg,
		HTTP:    httpCfg,

		Kafka:                 kafkaCfg,
		Sarama:                saramaCfg,
		ObjectReadConsumer:    objectReadConsumerCfg,
		ObjectWrittenConsumer: objectWrittenConsumerCfg,
		Retry:                 retryCfg,

		Migration: migrationCfg,
		Upstream:  upstreamCfg,
	}, nil
}
