package env

import (
	"fmt"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

type kafkaConfig struct {
	Bootstrap      string        `env:"KAFKA_BOOTSTRAP,required"`
	Topic          string        `env:"KAFKA_ACCESS_TOPIC" envDefault:"opens3.access.v1"`
	Group          string        `env:"KAFKA_GROUP_ID" envDefault:"sppr-ingestor-v1"`
	Retry          time.Duration `env:"KAFKA_RETRY_INTERVAL" envDefault:"1s"`
	ProcessTimeout time.Duration `env:"KAFKA_PROCESS_TIMEOUT" envDefault:"5s"`
	MaxBytes       int           `env:"KAFKA_MAX_EVENT_BYTES" envDefault:"65536"`
}

func NewKafkaConfig() (*kafkaConfig, error) {
	c := &kafkaConfig{}
	if err := env.Parse(c); err != nil {
		return nil, err
	}
	if strings.TrimSpace(c.Topic) == "" || strings.TrimSpace(c.Group) == "" || c.Retry <= 0 || c.ProcessTimeout <= 0 || c.MaxBytes < 1 {
		return nil, fmt.Errorf("invalid Kafka topic, group, retry interval, processing timeout or maximum event size")
	}
	for _, b := range c.Brokers() {
		if b == "" {
			return nil, fmt.Errorf("empty Kafka broker")
		}
	}
	return c, nil
}

func (c *kafkaConfig) Brokers() []string {
	b := strings.Split(c.Bootstrap, ",")
	for i := range b {
		b[i] = strings.TrimSpace(b[i])
	}
	return b
}
func (c *kafkaConfig) AccessTopic() string              { return c.Topic }
func (c *kafkaConfig) GroupID() string                  { return c.Group }
func (c *kafkaConfig) RetryInterval() time.Duration     { return c.Retry }
func (c *kafkaConfig) ProcessingTimeout() time.Duration { return c.ProcessTimeout }
func (c *kafkaConfig) MaxEventBytes() int               { return c.MaxBytes }
