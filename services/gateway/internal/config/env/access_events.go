package env

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type accessEventsConfig struct {
	enabled   bool
	gatewayID string
}

func NewAccessEventsConfig() (*accessEventsConfig, error) {
	var raw struct {
		Enabled   bool   `env:"ACCESS_EVENTS_ENABLED" envDefault:"false"`
		GatewayID string `env:"GATEWAY_ID"`
	}
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	if raw.Enabled && raw.GatewayID == "" {
		return nil, fmt.Errorf("GATEWAY_ID is required when ACCESS_EVENTS_ENABLED=true")
	}
	return &accessEventsConfig{enabled: raw.Enabled, gatewayID: raw.GatewayID}, nil
}

func (cfg *accessEventsConfig) Enabled() bool     { return cfg.enabled }
func (cfg *accessEventsConfig) GatewayID() string { return cfg.gatewayID }
