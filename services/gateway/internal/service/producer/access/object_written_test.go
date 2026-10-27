package access_test

import (
	"context"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestProduceObjectWritten(t *testing.T) {
	tests := []struct {
		name       string
		origin     model.Origin
		wantOrigin events_v1.Origin
	}{
		{name: "client write", origin: model.OriginClient, wantOrigin: events_v1.Origin_ORIGIN_CLIENT},
		{name: "migration write", origin: model.OriginMigration, wantOrigin: events_v1.Origin_ORIGIN_MIGRATION},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			var calls []sent
			s := newService(t, nil, recordingProducer(mc, &calls))

			s.ProduceObjectWritten(context.Background(), model.ObjectWrittenEvent{
				Bucket: "bucket", ObjectKey: "key", VersionID: "v2", Bytes: 9, Origin: tt.origin,
			})

			require.Len(t, calls, 1)
			msg := decodeWritten(t, calls[0].value)
			require.Equal(t, gatewayID+"/"+msg.GetPosition().GetStreamId(), calls[0].key)
			require.Equal(t, int64(1), msg.GetPosition().GetSequence())
			require.Equal(t, "bucket", msg.GetBucket())
			require.Equal(t, "key", msg.GetObjectKey())
			require.Equal(t, "v2", msg.GetVersionId())
			require.Equal(t, int64(9), msg.GetBytes())
			require.Equal(t, tt.wantOrigin, msg.GetOrigin())
			require.NotNil(t, msg.GetOccurredAt())
		})
	}
}
