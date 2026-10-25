package access_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestHandle(t *testing.T) {
	e := model.AccessEvent{SchemaVersion: 1, GatewayID: "gw", StreamID: "run", Sequence: 1, OccurredAt: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC), Bucket: "b", ObjectKey: "k", VersionID: "v", Operation: "read", Origin: "client", Result: "success"}
	body, err := json.Marshal(e)
	require.NoError(t, err)
	failure := errors.New("database unavailable")
	cases := []struct {
		name       string
		value      []byte
		key        string
		result     model.IngestResult
		serviceErr error
		wantErr    bool
		called     bool
		outcome    string
	}{
		{name: "stored", value: body, key: e.StreamKey(), called: true, outcome: "stored", result: model.IngestResult{Missing: 2}},
		{name: "duplicate", value: body, key: e.StreamKey(), called: true, outcome: "duplicate", result: model.IngestResult{Duplicate: true}},
		{name: "database error", value: body, key: e.StreamKey(), called: true, outcome: "error", serviceErr: failure, wantErr: true},
		{name: "malformed JSON", value: []byte(`{"schema_version":`), key: e.StreamKey(), outcome: "error", wantErr: true},
		{name: "trailing JSON", value: append(append([]byte{}, body...), []byte(` {}`)...), key: e.StreamKey(), outcome: "error", wantErr: true},
		{name: "unknown field", value: []byte(`{"future_field":1}`), key: e.StreamKey(), outcome: "error", wantErr: true},
		{name: "incorrect routing key", value: body, key: "other/run", outcome: "error", wantErr: true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			s := mocks.NewIngestorMock(mc)
			if tt.called {
				s.IngestMock.Set(func(_ context.Context, event model.AccessEvent, d model.Delivery) (model.IngestResult, error) {
					require.Equal(t, e, event)
					require.Equal(t, model.Delivery{Topic: "access", Offset: 10}, d)
					return tt.result, tt.serviceErr
				})
			}
			o := mocks.NewObserverMock(mc)
			o.ObserveMock.Set(func(_ context.Context, outcome string, missing int64, _ time.Duration) {
				require.Equal(t, tt.outcome, outcome)
				require.Equal(t, tt.result.Missing, missing)
			})
			h := access.NewHandler(s, o, noop.NewTracerProvider().Tracer("test"), 65536)
			err := h.Handle(context.Background(), kafka.Message{Topic: "access", Offset: 10, Key: []byte(tt.key), Value: tt.value})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if !tt.called {
				require.Equal(t, uint64(0), s.IngestAfterCounter())
			}
		})
	}
}
