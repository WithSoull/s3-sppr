package access_test

import (
	"context"
	"errors"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/converter/kafka/decoder"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/consumer/access"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	events "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

func TestHeartbeatHeaderDispatch(t *testing.T) {
	logger.SetNopLogger()
	down := errors.New("database unavailable")
	value, err := proto.Marshal(&events.StreamHeartbeat{Position: &events.StreamPosition{GatewayId: "gw", StreamId: "run"}, At: timestamppb.New(occurredAt), Closing: true})
	require.NoError(t, err)
	for _, tt := range []struct {
		name, header string
		value        []byte
		ingest       bool
		err, wantErr error
		outcome      string
	}{
		{name: "read-topic heartbeat", header: "heartbeat", value: value, ingest: true, outcome: "stored"},
		{name: "infrastructure holds partition", header: "heartbeat", value: value, ingest: true, err: down, wantErr: down, outcome: "error"},
		{name: "rejected commits", header: "heartbeat", value: value, ingest: true, err: model.ErrRejected, outcome: "rejected"},
		{name: "bad payload", header: "heartbeat", value: []byte{255}, outcome: "rejected"},
		{name: "unknown header", header: "future", value: value, outcome: "rejected"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			ing := mocks.NewIngestorMock(mc)
			observer := mocks.NewObserverMock(mc)
			if tt.ingest {
				ing.HeartbeatMock.Expect(minimock.AnyContext, model.StreamHeartbeat{GatewayID: "gw", StreamID: "run", At: occurredAt, Closing: true, Payload: value}, model.Delivery{Topic: "read"}).Return(model.IngestResult{}, tt.err)
			}
			observer.ObserveMock.Set(func(_ context.Context, outcome string, _ int64, _ time.Duration) {
				require.Equal(t, tt.outcome, outcome)
			})
			s := access.NewService(nil, decoder.NewObjectReadDecoder(), nil, decoder.NewObjectWrittenDecoder(), ing, observer)
			err := s.ObjectReadHandler(context.Background(), kafka.Message{Topic: "read", Headers: map[string][]byte{"event-type": []byte(tt.header)}, Value: tt.value})
			require.ErrorIs(t, err, tt.wantErr)
			require.Zero(t, ing.IngestAfterCounter())
			if !tt.ingest {
				require.Zero(t, ing.HeartbeatAfterCounter())
			}
		})
	}
}
