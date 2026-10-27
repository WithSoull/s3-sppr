package config

import (
	"time"

	"github.com/IBM/sarama"
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
}

type SaramaConfig interface {
	Config() *sarama.Config
	GroupID() string
}

type ObjectReadConsumerConfig interface {
	Topic() string
}

type ObjectWrittenConsumerConfig interface {
	Topic() string
}

type RetryConfig interface {
	Interval() time.Duration
}

type HTTPConfig interface {
	Address() string
	ShutdownTimeout() time.Duration
}

type MigrationConfig interface {
	Enabled() bool
	Workers() int
	MaxAttempts() int
	RetryBackoff() time.Duration
	SourceCleanupDelay() time.Duration
	CleanupInterval() time.Duration
	BudgetBytes() int64
	BudgetWindow() time.Duration
	MinInterval() time.Duration
}

type PlannerConfig interface {
	Enabled() bool
	Interval() time.Duration
	KeepPlans() int
	MaxRejectedSaved() int
	Horizon() time.Duration
	HotFixedMs() float64
	HotMiBPerMs() float64
	ColdFixedMs() float64
	ColdMiBPerMs() float64
	PenaltyMsPerMiB() float64
	MinGainMs() float64
	HotUtilization() float64
	ColdUtilization() float64
}

type UpstreamConfig interface {
	MetadataAddress() string
	MetadataTimeout() time.Duration
	StorageHotAddress() string
	StorageColdAddress() string
}
