package ingestor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/ingestor"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestIngest(t *testing.T) {
	ctx := context.Background()
	event := model.AccessEvent{SchemaVersion: 1, GatewayID: "gw", StreamID: "run", Sequence: 1,
		OccurredAt: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC), Bucket: "bucket", ObjectKey: "key", VersionID: "v1", Operation: "read", Origin: "client", Result: "success", Bytes: 100}
	delivery := model.Delivery{Topic: "access", Partition: 0, Offset: 0}
	repoErr := errors.New("repository unavailable")
	commitErr := errors.New("commit failed")
	invalid := event
	invalid.Sequence = 0
	withTimezone := event
	withTimezone.OccurredAt = event.OccurredAt.In(time.FixedZone("MSK", 3*60*60)).Add(789 * time.Nanosecond)
	type args struct {
		event    model.AccessEvent
		delivery model.Delivery
	}
	cases := []struct {
		name            string
		args            args
		want            model.IngestResult
		wantErr         error
		validationError bool
		build           func(*minimock.Controller, args) (*mocks.EventRepositoryMock, *mocks.TxManagerMock)
	}{
		{name: "stored with gap and normalized timestamp", args: args{withTimezone, delivery}, want: model.IngestResult{Missing: 2}, build: func(mc *minimock.Controller, a args) (*mocks.EventRepositoryMock, *mocks.TxManagerMock) {
			r := mocks.NewEventRepositoryMock(mc)
			a.event.OccurredAt = a.event.OccurredAt.UTC().Truncate(time.Microsecond)
			r.StoreMock.Expect(ctx, a.event, a.delivery).Return(model.IngestResult{Missing: 2}, nil)
			tx := mocks.NewTxManagerMock(mc)
			tx.ReadCommittedMock.Set(func(c context.Context, f db.Handler) error { return f(c) })
			return r, tx
		}},
		{name: "repository failure", args: args{event, delivery}, wantErr: repoErr, build: func(mc *minimock.Controller, a args) (*mocks.EventRepositoryMock, *mocks.TxManagerMock) {
			r := mocks.NewEventRepositoryMock(mc)
			r.StoreMock.Expect(ctx, a.event, a.delivery).Return(model.IngestResult{}, repoErr)
			tx := mocks.NewTxManagerMock(mc)
			tx.ReadCommittedMock.Set(func(c context.Context, f db.Handler) error { return f(c) })
			return r, tx
		}},
		{name: "commit failure hides uncommitted result", args: args{event, delivery}, wantErr: commitErr, build: func(mc *minimock.Controller, a args) (*mocks.EventRepositoryMock, *mocks.TxManagerMock) {
			r := mocks.NewEventRepositoryMock(mc)
			r.StoreMock.Expect(ctx, a.event, a.delivery).Return(model.IngestResult{Missing: 3}, nil)
			tx := mocks.NewTxManagerMock(mc)
			tx.ReadCommittedMock.Set(func(c context.Context, f db.Handler) error { require.NoError(t, f(c)); return commitErr })
			return r, tx
		}},
		{name: "invalid event does not access database", args: args{invalid, delivery}, validationError: true, build: func(mc *minimock.Controller, _ args) (*mocks.EventRepositoryMock, *mocks.TxManagerMock) {
			return mocks.NewEventRepositoryMock(mc), mocks.NewTxManagerMock(mc)
		}},
		{name: "invalid delivery does not access database", args: args{event, model.Delivery{}}, validationError: true, build: func(mc *minimock.Controller, _ args) (*mocks.EventRepositoryMock, *mocks.TxManagerMock) {
			return mocks.NewEventRepositoryMock(mc), mocks.NewTxManagerMock(mc)
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			r, tx := tt.build(mc, tt.args)
			result, err := ingestor.NewService(r, tx).Ingest(ctx, tt.args.event, tt.args.delivery)
			if tt.validationError {
				require.Error(t, err)
				require.Equal(t, uint64(0), r.StoreAfterCounter())
				require.Equal(t, uint64(0), tx.ReadCommittedAfterCounter())
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.Equal(t, tt.want, result)
		})
	}
}
