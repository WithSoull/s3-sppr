package access_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/converter/kafka/decoder"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/consumer/access"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var occurredAt = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

func objectRead(t *testing.T) []byte {
	t.Helper()
	value, err := proto.Marshal(&events_v1.ObjectRead{
		Position:   &events_v1.StreamPosition{GatewayId: "gw", StreamId: "run", Sequence: 7},
		OccurredAt: timestamppb.New(occurredAt),
		Bucket:     "bucket", ObjectKey: "key", VersionId: "v1", Bytes: 4,
		Origin: events_v1.Origin_ORIGIN_CLIENT,
	})
	require.NoError(t, err)
	return value
}

func TestObjectReadHandler(t *testing.T) {
	logger.SetNopLogger()
	value := objectRead(t)
	event := model.AccessEvent{GatewayID: "gw", StreamID: "run", Sequence: 7, OccurredAt: occurredAt,
		Bucket: "bucket", ObjectKey: "key", VersionID: "v1", Operation: model.OperationRead,
		Origin: model.OriginClient, Result: model.ResultSuccess, Bytes: 4, Payload: value}
	delivery := model.Delivery{Topic: "object-read", Partition: 2, Offset: 10}
	infraErr := errors.New("database unavailable")

	tests := []struct {
		name        string
		value       []byte
		ingest      bool
		result      model.IngestResult
		ingestErr   error
		wantErr     error
		wantOutcome string
		wantMissing int64
	}{
		{name: "stored with gap", value: value, ingest: true, result: model.IngestResult{Missing: 2}, wantOutcome: "stored", wantMissing: 2},
		{name: "duplicate", value: value, ingest: true, result: model.IngestResult{Duplicate: true}, wantOutcome: "duplicate"},
		{name: "rejected event is committed", value: value, ingest: true, ingestErr: fmt.Errorf("persist: %w", model.ErrRejected), wantOutcome: "rejected"},
		{name: "infrastructure error is retried", value: value, ingest: true, ingestErr: infraErr, wantErr: infraErr, wantOutcome: "error"},
		{name: "undecodable event is committed", value: []byte{0xff}, wantOutcome: "rejected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			ingestor := mocks.NewIngestorMock(mc)
			if tt.ingest {
				ingestor.IngestMock.Expect(minimock.AnyContext, event, delivery).Return(tt.result, tt.ingestErr)
			}
			observer := mocks.NewObserverMock(mc)
			observer.ObserveMock.Set(func(_ context.Context, outcome string, missing int64, _ time.Duration) {
				require.Equal(t, tt.wantOutcome, outcome)
				require.Equal(t, tt.wantMissing, missing)
			})
			s := access.NewService(nil, decoder.NewObjectReadDecoder(), nil, decoder.NewObjectWrittenDecoder(), ingestor, observer)

			err := s.ObjectReadHandler(context.Background(), kafka.Message{Topic: delivery.Topic, Partition: delivery.Partition, Offset: delivery.Offset, Value: tt.value})

			require.ErrorIs(t, err, tt.wantErr)
			if !tt.ingest {
				require.Zero(t, ingestor.IngestAfterCounter())
			}
			require.Equal(t, uint64(1), observer.ObserveAfterCounter())
		})
	}
}

func TestObjectWrittenHandlerUsesWriteDecoder(t *testing.T) {
	logger.SetNopLogger()
	value, err := proto.Marshal(&events_v1.ObjectWritten{
		Position:   &events_v1.StreamPosition{GatewayId: "gw", StreamId: "writes", Sequence: 1},
		OccurredAt: timestamppb.New(occurredAt),
		Bucket:     "bucket", ObjectKey: "key", VersionId: "v2", Bytes: 9,
		Origin: events_v1.Origin_ORIGIN_CLIENT,
	})
	require.NoError(t, err)
	mc := minimock.NewController(t)
	ingestor := mocks.NewIngestorMock(mc)
	ingestor.IngestMock.Set(func(_ context.Context, e model.AccessEvent, d model.Delivery) (model.IngestResult, error) {
		require.Equal(t, model.OperationWrite, e.Operation)
		require.Equal(t, "writes", e.StreamID)
		require.Equal(t, "object-written", d.Topic)
		return model.IngestResult{}, nil
	})
	observer := mocks.NewObserverMock(mc)
	observer.ObserveMock.Return()
	s := access.NewService(nil, decoder.NewObjectReadDecoder(), nil, decoder.NewObjectWrittenDecoder(), ingestor, observer)

	require.NoError(t, s.ObjectWrittenHandler(context.Background(), kafka.Message{Topic: "object-written", Value: value}))
	require.Equal(t, uint64(1), ingestor.IngestAfterCounter())
}
