package env

import (
	"fmt"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

type adminEnvConfig struct {
	Enabled      *bool         `env:"ADMIN_AUTH_ENABLED"`
	AuthAddress  string        `env:"AUTH_GRPC_ADDR"`
	AuthZAddress string        `env:"AUTHZ_GRPC_ADDR"`
	AuthTimeout  time.Duration `env:"AUTH_RPC_TIMEOUT" envDefault:"5s"`
	AuthZTimeout time.Duration `env:"AUTHZ_RPC_TIMEOUT" envDefault:"5s"`
}
type adminConfig struct {
	raw     adminEnvConfig
	enabled bool
}

func NewAdminConfig(plannerEnabled bool) (*adminConfig, error) {
	var raw adminEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	enabled := plannerEnabled
	if raw.Enabled != nil {
		enabled = *raw.Enabled
	}
	if raw.AuthTimeout <= 0 || raw.AuthZTimeout <= 0 {
		return nil, fmt.Errorf("AUTH_RPC_TIMEOUT and AUTHZ_RPC_TIMEOUT must be positive")
	}
	if enabled && (strings.TrimSpace(raw.AuthAddress) == "" || strings.TrimSpace(raw.AuthZAddress) == "") {
		return nil, fmt.Errorf("ADMIN_AUTH_ENABLED requires AUTH_GRPC_ADDR and AUTHZ_GRPC_ADDR")
	}
	return &adminConfig{raw, enabled}, nil
}
func (c *adminConfig) Enabled() bool               { return c.enabled }
func (c *adminConfig) AuthAddress() string         { return c.raw.AuthAddress }
func (c *adminConfig) AuthZAddress() string        { return c.raw.AuthZAddress }
func (c *adminConfig) AuthTimeout() time.Duration  { return c.raw.AuthTimeout }
func (c *adminConfig) AuthZTimeout() time.Duration { return c.raw.AuthZTimeout }
