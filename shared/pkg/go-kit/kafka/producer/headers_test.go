package producer_test

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/producer"
	"github.com/stretchr/testify/require"
)

func TestAsyncHeadersAndDeliveryReceipt(t *testing.T) {
	for _, tt := range []struct {
		name        string
		deliveryErr error
	}{{"ack", nil}, {"delivery failure", sarama.ErrOutOfBrokers}} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := sarama.NewConfig()
			cfg.Producer.Return.Successes = true
			ap := mocks.NewAsyncProducer(t, cfg)
			checker := func(msg *sarama.ProducerMessage) error {
				require.Equal(t, []sarama.RecordHeader{{Key: []byte("event-type"), Value: []byte("heartbeat")}}, msg.Headers)
				return nil
			}
			if tt.deliveryErr == nil {
				ap.ExpectInputWithMessageCheckerFunctionAndSucceed(checker)
			} else {
				ap.ExpectInputWithMessageCheckerFunctionAndFail(checker, tt.deliveryErr)
			}
			topics := make(chan string, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				producer.LogResults(ap, testLogger{}, func(topic string) { topics <- topic })
			}()
			receipt := make(chan error, 1)
			ctx := producer.WithReceipt(producer.WithHeaders(context.Background(), sarama.RecordHeader{Key: []byte("event-type"), Value: []byte("heartbeat")}), receipt)
			require.NoError(t, producer.NewAsyncProducer(ap, "read").Send(ctx, []byte("gw/stream"), []byte("heartbeat"), nil))
			select {
			case err := <-receipt:
				require.ErrorIs(t, err, tt.deliveryErr)
			case <-time.After(time.Second):
				t.Fatal("missing delivery receipt")
			}
			require.NoError(t, ap.Close())
			<-done
			if tt.deliveryErr != nil {
				require.Equal(t, "read", <-topics)
			} else {
				require.Empty(t, topics)
			}
		})
	}
}
