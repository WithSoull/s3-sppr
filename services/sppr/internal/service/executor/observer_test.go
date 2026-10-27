package executor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestTerminalMetricsRequireCommit(t *testing.T) {
	for _, tt := range []struct {
		name      string
		commitErr error
	}{{name: "committed"}, {name: "commit failed", commitErr: errors.New("commit failed")}} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			a := running(model.StepSwitching, model.ReasonNone, false)
			started := time.Now().Add(-time.Second)
			a.StartedAt = &started
			d := newDeps(mc, a)
			observer := mocks.NewMigrationObserverMock(mc)
			d.observer = observer
			target := reserved()
			target.State = model.CopyPromoted
			d.placements.ListMigrationCopiesMock.Return([]model.PlacementCopy{target}, nil)
			d.tx.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error {
				if err := f(ctx); err != nil {
					return err
				}
				return tt.commitErr
			})
			if tt.commitErr == nil {
				observer.ActionsMock.Expect(minimock.AnyContext, "succeeded", "", int64(1))
				observer.DurationMock.Set(func(_ context.Context, elapsed time.Duration) { require.GreaterOrEqual(t, elapsed, time.Second) })
			}
			_, err := d.executor().Step(context.Background())
			if tt.commitErr != nil {
				require.ErrorIs(t, err, tt.commitErr)
				require.Zero(t, observer.ActionsAfterCounter())
				require.Zero(t, observer.DurationAfterCounter())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCopyMetricsCountFailedAttempts(t *testing.T) {
	mc := minimock.NewController(t)
	d := newDeps(mc, running(model.StepReserving, model.ReasonNone, true))
	observer := mocks.NewMigrationObserverMock(mc)
	d.observer = observer
	fresh(d)
	d.placements.GetPlacementMock.Return(placement(1, time.Now()), nil)
	d.checks(false, 0)
	d.placements.ReserveCopyMock.Return(reserved(), nil)
	d.source(content)
	d.sink(unavailable(), nil)
	switches(d, nil)
	observer.CopiedMock.Expect(minimock.AnyContext, "HOT", size).Times(2)
	observer.ActionsMock.Expect(minimock.AnyContext, "succeeded", "", int64(1))
	_, err := d.executor().Step(context.Background())
	require.NoError(t, err)
}
