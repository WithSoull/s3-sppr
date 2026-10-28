package env

import (
	"time"

	"github.com/IBM/sarama"
)

type saramaConfig struct{}

func NewSaramaConfig() (*saramaConfig, error) {
	return &saramaConfig{}, nil
}

// Config is the AsyncProducer setup. Idempotence requires WaitForAll and one
// in-flight request; together they let sarama retry without duplicates or
// reordering. ChannelBufferSize bounds how many events wait before the first loss.
func (cfg *saramaConfig) Config() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.ChannelBufferSize = 1024
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 200 * time.Millisecond

	return config
}
