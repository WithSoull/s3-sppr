package access_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka/producer"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestProduceObjectRead(t *testing.T) {
	logger.SetNopLogger()
	event := model.ObjectReadEvent{Bucket: "bucket", ObjectKey: "a/b", VersionID: "v1", Bytes: 42, Origin: model.OriginClient}
	tests := []struct {
		name          string
		sendErrs      []error
		wantSequences []int64
	}{
		{name: "numbers start at one without holes", wantSequences: []int64{1, 2, 3}},
		{name: "full buffer spends the number", sendErrs: []error{nil, producer.ErrBufferFull}, wantSequences: []int64{1, 2, 3}},
		{name: "any send error spends the number", sendErrs: []error{errors.New("closed")}, wantSequences: []int64{1, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			var calls []sent
			s := newService(t, recordingProducer(mc, &calls, tt.sendErrs...), nil)
			before := time.Now().UTC()

			for range 3 {
				s.ProduceObjectRead(context.Background(), event)
			}

			require.Len(t, calls, len(tt.wantSequences))
			first := decodeRead(t, calls[0].value)
			streamID := first.GetPosition().GetStreamId()
			require.NotEmpty(t, streamID)
			require.Equal(t, gatewayID+"/"+streamID, calls[0].key)
			require.Equal(t, "bucket", first.GetBucket())
			require.Equal(t, "a/b", first.GetObjectKey())
			require.Equal(t, "v1", first.GetVersionId())
			require.Equal(t, int64(42), first.GetBytes())
			require.Equal(t, events_v1.Origin_ORIGIN_CLIENT, first.GetOrigin())
			require.False(t, first.GetOccurredAt().AsTime().Before(before.Truncate(time.Microsecond)))
			for i, call := range calls {
				msg := decodeRead(t, call.value)
				require.Equal(t, gatewayID, msg.GetPosition().GetGatewayId())
				require.Equal(t, streamID, msg.GetPosition().GetStreamId())
				require.Equal(t, tt.wantSequences[i], msg.GetPosition().GetSequence())
			}
		})
	}
}
