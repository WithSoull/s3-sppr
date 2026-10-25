package env

import (
	"strings"

	"github.com/caarlos0/env/v11"
)

type kafkaConfig struct{ brokers []string }

func NewKafkaConfig() (*kafkaConfig, error) {
	var raw struct {
		Brokers string `env:"KAFKA_BOOTSTRAP" envDefault:"localhost:9092"`
	}
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	return &kafkaConfig{brokers: strings.Split(raw.Brokers, ",")}, nil
}

func (cfg *kafkaConfig) Brokers() []string { return cfg.brokers }
