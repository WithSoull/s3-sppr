package env

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type accessEventsConfig struct {
	enabled           bool
	gatewayID         string
	heartbeatInterval time.Duration
}

func NewAccessEventsConfig() (*accessEventsConfig, error) {
	var raw struct {
		HeartbeatInterval time.Duration `env:"ACCESS_HEARTBEAT_INTERVAL" envDefault:"5s"`
		Enabled           bool          `env:"ACCESS_EVENTS_ENABLED" envDefault:"false"`
		GatewayID         string        `env:"GATEWAY_ID"`
	}
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	if raw.HeartbeatInterval <= 0 {
		return nil, fmt.Errorf("ACCESS_HEARTBEAT_INTERVAL must be positive")
	}
	if raw.Enabled && raw.GatewayID == "" {
		return nil, fmt.Errorf("GATEWAY_ID is required when ACCESS_EVENTS_ENABLED=true")
	}
	return &accessEventsConfig{enabled: raw.Enabled, gatewayID: raw.GatewayID, heartbeatInterval: raw.HeartbeatInterval}, nil
}

func (cfg *accessEventsConfig) Enabled() bool     { return cfg.enabled }
func (cfg *accessEventsConfig) GatewayID() string { return cfg.gatewayID }

func (cfg *accessEventsConfig) HeartbeatInterval() time.Duration { return cfg.heartbeatInterval }
