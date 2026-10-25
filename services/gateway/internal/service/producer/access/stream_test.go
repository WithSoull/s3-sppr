package access_test

import (
	"context"
	"sync"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

// Each topic has its own stream_id and counter: interleaved topics never leave
// holes in either stream.
func TestStreamsArePerTopic(t *testing.T) {
	mc := minimock.NewController(t)
	var reads, writes []sent
	s := newService(t, recordingProducer(mc, &reads), recordingProducer(mc, &writes))

	for range 2 {
		s.ProduceObjectRead(context.Background(), model.ObjectReadEvent{Bucket: "b", ObjectKey: "k", VersionID: "v"})
		s.ProduceObjectWritten(context.Background(), model.ObjectWrittenEvent{Bucket: "b", ObjectKey: "k", VersionID: "v"})
	}

	require.Len(t, reads, 2)
	require.Len(t, writes, 2)
	readStream := decodeRead(t, reads[0].value).GetPosition().GetStreamId()
	writeStream := decodeWritten(t, writes[0].value).GetPosition().GetStreamId()
	require.NotEqual(t, readStream, writeStream)
	for i := range 2 {
		require.Equal(t, int64(i+1), decodeRead(t, reads[i].value).GetPosition().GetSequence())
		require.Equal(t, int64(i+1), decodeWritten(t, writes[i].value).GetPosition().GetSequence())
	}
}

// Numbering, marshal and Send share the stream mutex, so concurrent requests
// reach the producer in sequence order with non-decreasing time. Without the
// mutex the unsynchronized append in recordingProducer also fails under -race.
func TestConcurrentEventsReachProducerInSequenceOrder(t *testing.T) {
	mc := minimock.NewController(t)
	var calls []sent
	s := newService(t, recordingProducer(mc, &calls), nil)
	const events = 128

	var wg sync.WaitGroup
	for range events {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.ProduceObjectRead(context.Background(), model.ObjectReadEvent{Bucket: "b", ObjectKey: "k", VersionID: "v"})
		}()
	}
	wg.Wait()

	require.Len(t, calls, events)
	for i, call := range calls {
		msg := decodeRead(t, call.value)
		require.Equal(t, int64(i+1), msg.GetPosition().GetSequence())
		if i > 0 {
			previous := decodeRead(t, calls[i-1].value).GetOccurredAt().AsTime()
			require.False(t, msg.GetOccurredAt().AsTime().Before(previous))
		}
	}
}
