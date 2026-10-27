package migration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/migration"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestCancelMetricsRequireCommit(t *testing.T) {
	for _, tt := range []struct {
		name      string
		commitErr error
	}{{name: "committed"}, {name: "commit failed", commitErr: errors.New("commit failed")}} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			actions := mocks.NewActionRepositoryMock(mc)
			tx := mocks.NewTxManagerMock(mc)
			observer := mocks.NewMigrationObserverMock(mc)
			actions.CancelPendingMock.Return([]string{"parent"}, nil)
			actions.CancelDependentsMock.Return([]string{"child", "grandchild"}, nil)
			tx.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error {
				if err := f(ctx); err != nil {
					return err
				}
				return tt.commitErr
			})
			if tt.commitErr == nil {
				observer.ActionsMock.When(minimock.AnyContext, "cancelled", "user", int64(1)).Then()
				observer.ActionsMock.When(minimock.AnyContext, "cancelled", "dependency_failed", int64(2)).Then()
			}
			svc := migration.NewService(actions, mocks.NewSettingsRepositoryMock(mc), mocks.NewPinRepositoryMock(mc), tx, observer)
			_, err := svc.CancelActions(context.Background(), model.CancelRequest{PlanID: "6c7f2a59-6a0a-4d55-9e0e-4a3f6f6b0009"})
			if tt.commitErr != nil {
				require.ErrorIs(t, err, tt.commitErr)
				require.Zero(t, observer.ActionsAfterCounter())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSubmitCancellationMetrics(t *testing.T) {
	cases := []struct {
		name      string
		commitErr error
	}{{name: "committed"}, {name: "commit failed", commitErr: errors.New("commit failed")}}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			actions := mocks.NewActionRepositoryMock(mc)
			tx := mocks.NewTxManagerMock(mc)
			observer := mocks.NewMigrationObserverMock(mc)
			actions.GetManyMock.Return(map[string]*model.Action{actionA: accepted(spec(actionA), model.ActionFailed)}, nil)
			actions.InsertMock.Expect(minimock.AnyContext, withPlan(spec(actionB, actionA))).Return(nil)
			actions.CancelDependentsMock.Expect(minimock.AnyContext, []string{actionA}).Return([]string{actionB}, nil)
			tx.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error {
				if err := f(ctx); err != nil {
					return err
				}
				return tt.commitErr
			})
			if tt.commitErr == nil {
				observer.ActionsMock.Expect(minimock.AnyContext, "cancelled", "dependency_failed", int64(1))
			}
			svc := migration.NewService(actions, mocks.NewSettingsRepositoryMock(mc), mocks.NewPinRepositoryMock(mc), tx, observer)
			_, err := svc.SubmitActions(context.Background(), planID, []model.ActionSpec{spec(actionB, actionA)})
			if tt.commitErr != nil {
				require.ErrorIs(t, err, tt.commitErr)
				require.Zero(t, observer.ActionsAfterCounter())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
