package env

import "github.com/caarlos0/env/v11"

type objectWrittenProducerConfig struct{ topic string }

func NewObjectWrittenProducerConfig() (*objectWrittenProducerConfig, error) {
	var raw struct {
		Topic string `env:"KAFKA_OBJECT_WRITTEN_TOPIC" envDefault:"object-written"`
	}
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	return &objectWrittenProducerConfig{topic: raw.Topic}, nil
}

func (cfg *objectWrittenProducerConfig) Topic() string { return cfg.topic }
