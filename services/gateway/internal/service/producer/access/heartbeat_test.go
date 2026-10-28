package access_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	saramamocks "github.com/IBM/sarama/mocks"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service/producer/access"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	producer "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/producer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	events "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

func TestHeartbeatAndGracefulStop(t *testing.T) {
	mc := minimock.NewController(t)
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	transport := saramamocks.NewAsyncProducer(t, cfg)
	type record struct {
		topic     string
		heartbeat bool
		seq       int64
		closing   bool
	}
	var records []record
	for range 8 {
		transport.ExpectInputWithMessageCheckerFunctionAndSucceed(func(m *sarama.ProducerMessage) error {
			b, _ := m.Value.Encode()
			rec := record{topic: m.Topic}
			if len(m.Headers) > 0 {
				require.Equal(t, "event-type", string(m.Headers[0].Key))
				require.Equal(t, "heartbeat", string(m.Headers[0].Value))
				var h events.StreamHeartbeat
				require.NoError(t, proto.Unmarshal(b, &h))
				rec.heartbeat = true
				rec.seq = h.Position.Sequence
				rec.closing = h.Closing
			} else {
				var e events.ObjectRead
				require.NoError(t, proto.Unmarshal(b, &e))
				rec.seq = e.Position.Sequence
			}
			records = append(records, rec)
			return nil
		})
	}
	logger.SetNopLogger()
	done := make(chan struct{})
	go func() { defer close(done); producer.LogResults(transport, logger.Logger(), func(string) {}) }()
	wrap := func(topic string) *mocks.ProducerMock {
		p := mocks.NewProducerMock(mc)
		real := producer.NewAsyncProducer(transport, topic)
		p.SendMock.Set(real.Send)
		return p
	}
	dropped, _ := noop.NewMeterProvider().Meter("test").Int64Counter("dropped")
	s := access.NewService(gatewayID, wrap("reads"), wrap("writes"), dropped)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.ProduceObjectRead(ctx, model.ObjectReadEvent{})
	require.NoError(t, s.Heartbeat(ctx, false, false))
	s.ProduceObjectRead(ctx, model.ObjectReadEvent{})
	require.NoError(t, s.Stop(ctx))
	require.NoError(t, transport.Close())
	<-done
	var reads, writes []record
	for _, r := range records {
		if r.topic == "reads" {
			reads = append(reads, r)
		} else {
			writes = append(writes, r)
		}
	}
	require.Equal(t, []record{{"reads", false, 1, false}, {"reads", true, 1, false}, {"reads", false, 2, false}, {"reads", true, 2, false}, {"reads", true, 2, true}}, reads)
	require.Equal(t, []record{{"writes", true, 0, false}, {"writes", true, 0, false}, {"writes", true, 0, true}}, writes)
}
func TestConcurrentHeartbeatSharesEventMutex(t *testing.T) {
	mc := minimock.NewController(t)
	var calls []sent
	read := recordingProducer(mc, &calls)
	write := mocks.NewProducerMock(mc)
	write.SendMock.Return(nil)
	counter, _ := noop.NewMeterProvider().Meter("test").Int64Counter("dropped")
	s := access.NewService(gatewayID, read, write, counter)
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(2)
		go func() { defer wg.Done(); s.ProduceObjectRead(context.Background(), model.ObjectReadEvent{}) }()
		go func() { defer wg.Done(); require.NoError(t, s.Heartbeat(context.Background(), false, false)) }()
	}
	wg.Wait()
	last := int64(0)
	for _, c := range calls {
		var h events.StreamHeartbeat
		require.NoError(t, proto.Unmarshal(c.value, &h))
		if len(c.value) > 0 { // both messages share position and timestamp tags; event has extra fields
			if h.Position.Sequence == last+1 {
				last++
			} else {
				require.Equal(t, last, h.Position.Sequence)
			}
		}
	}
	require.Equal(t, int64(64), last)
}
