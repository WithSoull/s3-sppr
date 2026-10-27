package access_test

import (
	"context"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service/producer/access"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

const gatewayID = "gateway-1"

type sent struct {
	key   string
	value []byte
}

// recordingProducer keeps every Send in call order and returns errs in turn.
// Send runs under the stream mutex, so the slice needs no lock of its own.
func recordingProducer(mc *minimock.Controller, calls *[]sent, errs ...error) *mocks.ProducerMock {
	producer := mocks.NewProducerMock(mc)
	producer.SendMock.Set(func(_ context.Context, key, value []byte, _ kafka.PrettyDecoder) error {
		*calls = append(*calls, sent{key: string(key), value: value})
		if len(errs) < len(*calls) {
			return nil
		}
		return errs[len(*calls)-1]
	})
	return producer
}

func newService(t *testing.T, objectRead, objectWritten kafka.Producer) service.AccessProducerService {
	t.Helper()
	dropped, err := noop.NewMeterProvider().Meter("test").Int64Counter("dropped")
	require.NoError(t, err)
	return access.NewService(gatewayID, objectRead, objectWritten, dropped)
}

func decodeRead(t *testing.T, value []byte) *events_v1.ObjectRead {
	t.Helper()
	var msg events_v1.ObjectRead
	require.NoError(t, proto.Unmarshal(value, &msg))
	return &msg
}

func decodeWritten(t *testing.T, value []byte) *events_v1.ObjectWritten {
	t.Helper()
	var msg events_v1.ObjectWritten
	require.NoError(t, proto.Unmarshal(value, &msg))
	return &msg
}
