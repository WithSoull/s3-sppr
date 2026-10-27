package producer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/producer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testLogger struct{}

func (testLogger) Info(context.Context, string, ...zap.Field)  {}
func (testLogger) Error(context.Context, string, ...zap.Field) {}

func TestAsyncSendHandsMessageToTopic(t *testing.T) {
	transport := mocks.NewAsyncProducer(t, sarama.NewConfig())
	transport.ExpectInputWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		key, _ := msg.Key.Encode()
		value, _ := msg.Value.Encode()
		if msg.Topic != "object-read" || string(key) != "gw/run" || string(value) != "payload" {
			return errors.New("unexpected message")
		}
		return nil
	})

	p := producer.NewAsyncProducer(transport, "object-read")

	require.NoError(t, p.Send(context.Background(), []byte("gw/run"), []byte("payload"), nil))
	require.NoError(t, transport.Close())
}

func TestAsyncSendReturnsBufferFullWithoutBlocking(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.ChannelBufferSize = 1
	transport := mocks.NewAsyncProducer(t, cfg)
	entered, release := make(chan struct{}), make(chan struct{})
	transport.ExpectInputWithMessageCheckerFunctionAndSucceed(func(*sarama.ProducerMessage) error {
		close(entered)
		<-release
		return nil
	})
	transport.ExpectInputAndSucceed()

	p := producer.NewAsyncProducer(transport, "object-read")

	require.NoError(t, p.Send(context.Background(), nil, []byte("1"), nil))
	<-entered
	// The mock is busy with message 1 and its one-slot buffer holds message 2.
	require.NoError(t, p.Send(context.Background(), nil, []byte("2"), nil))
	start := time.Now()
	err := p.Send(context.Background(), nil, []byte("3"), nil)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, producer.ErrBufferFull)
	require.Less(t, elapsed, 100*time.Millisecond, "Send must not wait for Kafka")
	close(release)
	require.NoError(t, transport.Close())
}

func TestLogErrorsReportsTopicUntilClose(t *testing.T) {
	transport := mocks.NewAsyncProducer(t, sarama.NewConfig())
	transport.ExpectInputAndFail(sarama.ErrOutOfBrokers)
	topics := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		producer.LogErrors(transport, testLogger{}, func(topic string) { topics <- topic })
	}()

	require.NoError(t, producer.NewAsyncProducer(transport, "object-written").Send(context.Background(), nil, []byte("event"), nil))

	require.Equal(t, "object-written", <-topics)
	require.NoError(t, transport.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("LogErrors must return after the producer is closed")
	}
}
