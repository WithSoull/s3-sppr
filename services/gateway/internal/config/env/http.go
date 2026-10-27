package env

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type httpConfig struct {
	address         string
	maxReadDuration time.Duration
}

func NewHTTPConfig() (*httpConfig, error) {
	var raw struct {
		Address         string        `env:"GATEWAY_HTTP_ADDR" envDefault:"127.0.0.1:8080"`
		MaxReadDuration time.Duration `env:"GATEWAY_MAX_READ_DURATION" envDefault:"5m"`
	}
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	if raw.MaxReadDuration <= 0 {
		return nil, fmt.Errorf("GATEWAY_MAX_READ_DURATION must be positive")
	}
	return &httpConfig{address: raw.Address, maxReadDuration: raw.MaxReadDuration}, nil
}

func (cfg *httpConfig) Address() string                { return cfg.address }
func (cfg *httpConfig) MaxReadDuration() time.Duration { return cfg.maxReadDuration }
