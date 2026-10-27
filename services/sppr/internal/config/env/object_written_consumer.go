package env

import (
	"github.com/caarlos0/env/v11"
)

type objectWrittenConsumerEnvConfig struct {
	TopicName string `env:"KAFKA_OBJECT_WRITTEN_TOPIC" envDefault:"object-written"`
}

type objectWrittenConsumerConfig struct {
	raw objectWrittenConsumerEnvConfig
}

func NewObjectWrittenConsumerConfig() (*objectWrittenConsumerConfig, error) {
	var raw objectWrittenConsumerEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}

	return &objectWrittenConsumerConfig{raw: raw}, nil
}

func (cfg *objectWrittenConsumerConfig) Topic() string {
	return cfg.raw.TopicName
}
