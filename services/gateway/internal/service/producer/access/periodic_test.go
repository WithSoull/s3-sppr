package access

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestPeriodicHeartbeatStartsBothIdleStreams(t *testing.T) {
	mc := minimock.NewController(t)
	read, written := mocks.NewProducerMock(mc), mocks.NewProducerMock(mc)
	reads, writes := make(chan struct{}, 1), make(chan struct{}, 1)
	record := func(ch chan struct{}) func(context.Context, []byte, []byte, kafka.PrettyDecoder) error {
		return func(context.Context, []byte, []byte, kafka.PrettyDecoder) error {
			select {
			case ch <- struct{}{}:
			default:
			}
			return nil
		}
	}
	read.SendMock.Set(record(reads))
	written.SendMock.Set(record(writes))
	counter, _ := noop.NewMeterProvider().Meter("test").Int64Counter("dropped")
	s := NewService("gw", read, written, counter)
	s.Start(time.Millisecond)
	defer func() { s.stopOnce.Do(func() { close(s.stop) }); <-s.done }()
	for _, ch := range []chan struct{}{reads, writes} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("idle stream heartbeat not sent")
		}
	}
	require.Equal(t, int64(0), s.objectReadStream.sequence)
	require.Equal(t, int64(0), s.objectWrittenStream.sequence)
}

func TestAbortDoesNotPublishClosingHeartbeat(t *testing.T) {
	mc := minimock.NewController(t)
	read, written := mocks.NewProducerMock(mc), mocks.NewProducerMock(mc)
	counter, _ := noop.NewMeterProvider().Meter("test").Int64Counter("dropped")
	s := NewService("gw", read, written, counter)
	require.NoError(t, s.Abort(context.Background()))
	require.True(t, s.objectReadStream.closed)
	require.True(t, s.objectWrittenStream.closed)
	require.Zero(t, read.SendAfterCounter())
	require.Zero(t, written.SendAfterCounter())
}
