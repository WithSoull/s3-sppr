package env

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Addresses default to the host ports Gateway uses.
type upstreamEnvConfig struct {
	MetadataAddress    string        `env:"METADATA_GRPC_ADDR" envDefault:"localhost:50052"`
	MetadataTimeout    time.Duration `env:"METADATA_RPC_TIMEOUT" envDefault:"5s"`
	StorageHotAddress  string        `env:"STORAGE_HOT_GRPC_ADDR" envDefault:"localhost:50056"`
	StorageColdAddress string        `env:"STORAGE_COLD_GRPC_ADDR" envDefault:"localhost:50053"`
}

type upstreamConfig struct {
	raw upstreamEnvConfig
}

func NewUpstreamConfig() (*upstreamConfig, error) {
	var raw upstreamEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	if raw.MetadataTimeout <= 0 {
		return nil, fmt.Errorf("METADATA_RPC_TIMEOUT must be positive")
	}
	return &upstreamConfig{raw: raw}, nil
}

func (c *upstreamConfig) MetadataAddress() string        { return c.raw.MetadataAddress }
func (c *upstreamConfig) MetadataTimeout() time.Duration { return c.raw.MetadataTimeout }
func (c *upstreamConfig) StorageHotAddress() string      { return c.raw.StorageHotAddress }
func (c *upstreamConfig) StorageColdAddress() string     { return c.raw.StorageColdAddress }
