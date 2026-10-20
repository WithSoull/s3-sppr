package env

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type metricsEnvConfig struct {
	Enabled     bool          `env:"OTEL_ENABLED" envDefault:"false"`
	ServiceName string        `env:"OTEL_SERVICE_NAME" envDefault:"gateway"`
	Version     string        `env:"OTEL_SERVICE_VERSION" envDefault:"dev"`
	Environment string        `env:"OTEL_ENVIRONMENT" envDefault:"local"`
	Endpoint    string        `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
	PushTimeout time.Duration `env:"OTEL_METRICS_PUSH_TIMEOUT" envDefault:"30s"`
}

type metricsConfig struct{ raw metricsEnvConfig }

func NewMetricsConfig() (*metricsConfig, error) {
	var raw metricsEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	if raw.ServiceName == "" || raw.Endpoint == "" || raw.PushTimeout <= 0 {
		return nil, fmt.Errorf("invalid metrics configuration: service name, endpoint and positive push timeout are required")
	}
	return &metricsConfig{raw: raw}, nil
}

func (cfg *metricsConfig) Enabled() bool              { return cfg.raw.Enabled }
func (cfg *metricsConfig) ServiceName() string        { return cfg.raw.ServiceName }
func (cfg *metricsConfig) ServiceVersion() string     { return cfg.raw.Version }
func (cfg *metricsConfig) ServiceEnvironment() string { return cfg.raw.Environment }
func (cfg *metricsConfig) OTLPEndpoint() string       { return cfg.raw.Endpoint }
func (cfg *metricsConfig) PushTimeout() time.Duration { return cfg.raw.PushTimeout }
