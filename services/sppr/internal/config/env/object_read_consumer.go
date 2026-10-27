package env

import (
	"github.com/caarlos0/env/v11"
)

type objectReadConsumerEnvConfig struct {
	TopicName string `env:"KAFKA_OBJECT_READ_TOPIC" envDefault:"object-read"`
}

type objectReadConsumerConfig struct {
	raw objectReadConsumerEnvConfig
}

func NewObjectReadConsumerConfig() (*objectReadConsumerConfig, error) {
	var raw objectReadConsumerEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}

	return &objectReadConsumerConfig{raw: raw}, nil
}

func (cfg *objectReadConsumerConfig) Topic() string {
	return cfg.raw.TopicName
}
