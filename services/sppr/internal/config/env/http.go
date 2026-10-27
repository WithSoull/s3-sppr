package env

import (
	"fmt"
	"net"
	"time"

	"github.com/caarlos0/env/v11"
)

type httpConfig struct {
	Addr     string        `env:"HTTP_ADDRESS" envDefault:"0.0.0.0:8086"`
	Shutdown time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`
}

func NewHTTPConfig() (*httpConfig, error) {
	c := &httpConfig{}
	if err := env.Parse(c); err != nil {
		return nil, err
	}
	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		return nil, fmt.Errorf("HTTP_ADDRESS: %w", err)
	}
	if c.Shutdown <= 0 {
		return nil, fmt.Errorf("SHUTDOWN_TIMEOUT must be positive")
	}
	return c, nil
}
func (c *httpConfig) Address() string                { return c.Addr }
func (c *httpConfig) ShutdownTimeout() time.Duration { return c.Shutdown }
