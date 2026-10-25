package config

import (
	"time"
)

type PGConfig interface {
	DSN() string
	Timeout() time.Duration
	NeedLog() bool
}

type LoggerConfig interface {
	LogLevel() string
	AsJSON() bool
	EnableOLTP() bool
	ServiceName() string
	OTLPEndpoint() string
	ServiceEnvironment() string
}

type TracingConfig interface {
	CollectorEndpoint() string
	ServiceName() string
	Environment() string
	ServiceVersion() string
}

type MetricsConfig interface {
	ServiceName() string
	ServiceVersion() string
	OTLPEndpoint() string
	ServiceEnvironment() string
	PushTimeout() time.Duration
}

type KafkaConfig interface {
	Brokers() []string
	AccessTopic() string
	GroupID() string
	RetryInterval() time.Duration
	ProcessingTimeout() time.Duration
	MaxEventBytes() int
}

type HTTPConfig interface {
	Address() string
	ShutdownTimeout() time.Duration
}
