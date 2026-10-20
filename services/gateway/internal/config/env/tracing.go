package env

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type tracingEnvConfig struct {
	Enabled     bool   `env:"OTEL_ENABLED" envDefault:"false"`
	ServiceName string `env:"OTEL_SERVICE_NAME" envDefault:"gateway"`
	Version     string `env:"OTEL_SERVICE_VERSION" envDefault:"dev"`
	Environment string `env:"OTEL_ENVIRONMENT" envDefault:"local"`
	Endpoint    string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
}

type tracingConfig struct{ raw tracingEnvConfig }

func NewTracingConfig() (*tracingConfig, error) {
	var raw tracingEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	if raw.ServiceName == "" || raw.Endpoint == "" {
		return nil, fmt.Errorf("invalid tracing configuration: service name and endpoint are required")
	}
	return &tracingConfig{raw: raw}, nil
}

func (cfg *tracingConfig) Enabled() bool             { return cfg.raw.Enabled }
func (cfg *tracingConfig) ServiceName() string       { return cfg.raw.ServiceName }
func (cfg *tracingConfig) ServiceVersion() string    { return cfg.raw.Version }
func (cfg *tracingConfig) Environment() string       { return cfg.raw.Environment }
func (cfg *tracingConfig) CollectorEndpoint() string { return cfg.raw.Endpoint }
