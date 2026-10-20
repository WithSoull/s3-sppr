package env

import "github.com/caarlos0/env/v11"

type loggerEnvConfig struct {
	Level       string `env:"LOGGER_LEVEL" envDefault:"info"`
	AsJSON      bool   `env:"LOGGER_AS_JSON" envDefault:"false"`
	EnableOTLP  bool   `env:"LOGGER_ENABLE_OLTP" envDefault:"false"`
	ServiceName string `env:"OTEL_SERVICE_NAME" envDefault:"gateway"`
	Environment string `env:"OTEL_ENVIRONMENT" envDefault:"local"`
	Endpoint    string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
}

type loggerConfig struct{ raw loggerEnvConfig }

func NewLoggerConfig() (*loggerConfig, error) {
	var raw loggerEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	return &loggerConfig{raw: raw}, nil
}

func (cfg *loggerConfig) LogLevel() string           { return cfg.raw.Level }
func (cfg *loggerConfig) AsJSON() bool               { return cfg.raw.AsJSON }
func (cfg *loggerConfig) EnableOLTP() bool           { return cfg.raw.EnableOTLP }
func (cfg *loggerConfig) ServiceName() string        { return cfg.raw.ServiceName }
func (cfg *loggerConfig) ServiceEnvironment() string { return cfg.raw.Environment }
func (cfg *loggerConfig) OTLPEndpoint() string       { return cfg.raw.Endpoint }
