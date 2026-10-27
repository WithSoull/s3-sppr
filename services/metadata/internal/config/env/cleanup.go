package env

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type cleanupEnvConfig struct {
	DeleteCleanupDelay time.Duration `env:"METADATA_DELETE_CLEANUP_DELAY" envDefault:"10m"`
	// Same variable as in Gateway: Compose passes one value to both services.
	MaxReadDuration time.Duration `env:"GATEWAY_MAX_READ_DURATION" envDefault:"5m"`
}

type cleanupConfig struct {
	raw cleanupEnvConfig
}

func NewCleanupConfig() (*cleanupConfig, error) {
	var raw cleanupEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}

	if raw.MaxReadDuration <= 0 {
		return nil, fmt.Errorf("GATEWAY_MAX_READ_DURATION must be positive")
	}
	// A GET that started before DELETE must be able to finish reading the blob.
	if raw.DeleteCleanupDelay <= raw.MaxReadDuration {
		return nil, fmt.Errorf(
			"METADATA_DELETE_CLEANUP_DELAY (%s) must exceed GATEWAY_MAX_READ_DURATION (%s)",
			raw.DeleteCleanupDelay, raw.MaxReadDuration,
		)
	}

	return &cleanupConfig{raw: raw}, nil
}

func (cfg *cleanupConfig) DeleteCleanupDelay() time.Duration {
	return cfg.raw.DeleteCleanupDelay
}
