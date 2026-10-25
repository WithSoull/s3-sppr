package env

import "github.com/caarlos0/env/v11"

type objectReadProducerConfig struct{ topic string }

func NewObjectReadProducerConfig() (*objectReadProducerConfig, error) {
	var raw struct {
		Topic string `env:"KAFKA_OBJECT_READ_TOPIC" envDefault:"object-read"`
	}
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	return &objectReadProducerConfig{topic: raw.Topic}, nil
}

func (cfg *objectReadProducerConfig) Topic() string { return cfg.topic }
