package env

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type retryEnvConfig struct {
	Interval time.Duration `env:"KAFKA_RETRY_INTERVAL" envDefault:"1s"`
}

type retryConfig struct {
	raw retryEnvConfig
}

func NewRetryConfig() (*retryConfig, error) {
	var raw retryEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	if raw.Interval <= 0 {
		return nil, fmt.Errorf("KAFKA_RETRY_INTERVAL must be positive")
	}

	return &retryConfig{raw: raw}, nil
}

func (cfg *retryConfig) Interval() time.Duration {
	return cfg.raw.Interval
}
