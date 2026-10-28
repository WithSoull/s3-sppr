package ingestor_test

import (
	"context"
	"errors"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/ingestor"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHeartbeat(t *testing.T) {
	h := model.StreamHeartbeat{GatewayID: "gw", StreamID: "run", At: time.Date(2026, 10, 28, 12, 0, 0, 123, time.UTC)}
	d := model.Delivery{Topic: "reads"}
	down := errors.New("database down")
	commit := errors.New("commit failed")
	for _, tt := range []struct {
		name               string
		sequence           int64
		result             model.IngestResult
		repoErr, commitErr error
		invalid            bool
	}{
		{name: "zero sequence before any event"}, {name: "tail gap", sequence: 4, result: model.IngestResult{Missing: 3}}, {name: "replay", sequence: 4, result: model.IngestResult{Duplicate: true}}, {name: "database failure", repoErr: down}, {name: "commit failure", result: model.IngestResult{Missing: 3}, commitErr: commit}, {name: "validation does not call repository", sequence: -1, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			r := mocks.NewEventRepositoryMock(mc)
			tx := mocks.NewTxManagerMock(mc)
			arg := h
			arg.Sequence = tt.sequence
			if !tt.invalid {
				normalized := arg
				normalized.At = arg.At.Truncate(time.Microsecond)
				r.StoreHeartbeatMock.Expect(minimock.AnyContext, normalized, d).Return(tt.result, tt.repoErr)
				tx.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error {
					if err := f(ctx); err != nil {
						return err
					}
					return tt.commitErr
				})
			}
			result, err := ingestor.NewService(r, tx).Heartbeat(context.Background(), arg, d)
			if tt.invalid {
				require.ErrorIs(t, err, model.ErrRejected)
				require.Zero(t, r.StoreHeartbeatAfterCounter())
				require.Zero(t, tx.ReadCommittedAfterCounter())
			} else if tt.repoErr != nil {
				require.ErrorIs(t, err, tt.repoErr)
			} else {
				require.ErrorIs(t, err, tt.commitErr)
			}
			if err != nil {
				require.Empty(t, result)
			} else {
				require.Equal(t, tt.result, result)
			}
		})
	}
}
func TestLostStreamUsesInjectedTimeAndTransaction(t *testing.T) {
	mc := minimock.NewController(t)
	r := mocks.NewEventRepositoryMock(mc)
	tx := mocks.NewTxManagerMock(mc)
	at := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.CloseLostStreamsMock.Expect(minimock.AnyContext, at, 30*time.Second).Return(nil)
	tx.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error { err := f(ctx); cancel(); return err })
	ingestor.RunLostCheck(ctx, ingestor.NewService(r, tx), 30*time.Second, func() time.Time { return at })
	require.Equal(t, uint64(1), r.CloseLostStreamsAfterCounter())
}
