package config

import "time"

type AccessConfig interface {
	Enabled() bool
	GatewayID() string
	Brokers() []string
	AccessTopic() string
	QueueCapacity() int
	RetryInterval() time.Duration
	SendTimeout() time.Duration
	ShutdownTimeout() time.Duration
}

type HTTPConfig interface{ Address() string }
type GRPCClientConfig interface{ Address() string }
type MetadataClientConfig interface {
	GRPCClientConfig
	Timeout() time.Duration
}
type StorageClientConfig interface {
	GRPCClientConfig
	RetrieveChunkSizeBytes() int
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
	Enabled() bool
	ServiceName() string
	ServiceVersion() string
	Environment() string
	CollectorEndpoint() string
}

type MetricsConfig interface {
	Enabled() bool
	ServiceName() string
	ServiceVersion() string
	ServiceEnvironment() string
	OTLPEndpoint() string
	PushTimeout() time.Duration
}
