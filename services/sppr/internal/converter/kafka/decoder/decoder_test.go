package decoder_test

import (
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/converter/kafka/decoder"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var occurredAt = time.Date(2026, 10, 21, 12, 0, 0, 123456789, time.UTC)

func TestObjectReadDecoder(t *testing.T) {
	position := &events_v1.StreamPosition{GatewayId: "gw", StreamId: "run", Sequence: 3}
	tests := []struct {
		name    string
		message *events_v1.ObjectRead
		raw     []byte
		want    model.AccessEvent
		wantErr bool
	}{
		{
			name: "client read",
			message: &events_v1.ObjectRead{Position: position, OccurredAt: timestamppb.New(occurredAt),
				Bucket: "bucket", ObjectKey: "a/b", VersionId: "v1", Bytes: 42, Origin: events_v1.Origin_ORIGIN_CLIENT},
			want: model.AccessEvent{GatewayID: "gw", StreamID: "run", Sequence: 3, OccurredAt: occurredAt,
				Bucket: "bucket", ObjectKey: "a/b", VersionID: "v1", Operation: model.OperationRead,
				Origin: model.OriginClient, Result: model.ResultSuccess, Bytes: 42},
		},
		{
			name:    "migration read",
			message: &events_v1.ObjectRead{Position: position, OccurredAt: timestamppb.New(occurredAt), Origin: events_v1.Origin_ORIGIN_MIGRATION},
			want: model.AccessEvent{GatewayID: "gw", StreamID: "run", Sequence: 3, OccurredAt: occurredAt,
				Operation: model.OperationRead, Origin: model.OriginMigration, Result: model.ResultSuccess},
		},
		{
			name:    "missing fields stay invalid",
			message: &events_v1.ObjectRead{},
			want:    model.AccessEvent{Operation: model.OperationRead, Result: model.ResultSuccess},
		},
		{name: "not protobuf", raw: []byte{0xff}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.raw
			if tt.message != nil {
				var err error
				data, err = proto.Marshal(tt.message)
				require.NoError(t, err)
			}

			got, err := decoder.NewObjectReadDecoder().Decode(data)

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			tt.want.Payload = data
			require.Equal(t, tt.want, got)
		})
	}
}

func TestObjectWrittenDecoder(t *testing.T) {
	data, err := proto.Marshal(&events_v1.ObjectWritten{
		Position:   &events_v1.StreamPosition{GatewayId: "gw", StreamId: "writes", Sequence: 1},
		OccurredAt: timestamppb.New(occurredAt),
		Bucket:     "bucket", ObjectKey: "key", VersionId: "v2", Bytes: 9,
		Origin: events_v1.Origin_ORIGIN_CLIENT,
	})
	require.NoError(t, err)

	got, err := decoder.NewObjectWrittenDecoder().Decode(data)

	require.NoError(t, err)
	require.Equal(t, model.AccessEvent{GatewayID: "gw", StreamID: "writes", Sequence: 1, OccurredAt: occurredAt,
		Bucket: "bucket", ObjectKey: "key", VersionID: "v2", Operation: model.OperationWrite,
		Origin: model.OriginClient, Result: model.ResultSuccess, Bytes: 9, Payload: data}, got)
	_, err = decoder.NewObjectWrittenDecoder().Decode([]byte{0xff})
	require.Error(t, err)
}
