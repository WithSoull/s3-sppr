package tests

import (
	"context"
	"testing"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	placementService "github.com/alesplll/opens3-rebac/services/metadata/internal/service/placement"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestCompleteCleanup(t *testing.T) {
	type args struct {
		deleted      bool
		errorMessage string
	}

	locked := func(c *model.PlacementCopy) func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
		return func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
			m := mocks.NewCopyRepositoryMock(mc)
			m.LockByIDMock.Expect(minimock.AnyContext, copyID).Return(c, nil)
			return m
		}
	}

	tests := []struct {
		name          string
		args          args
		want          *model.PlacementCopy
		wantErr       error
		buildCopyMock func(mc *minimock.Controller) *mocks.CopyRepositoryMock
	}{
		{
			name: "success: deleted blob releases space",
			args: args{deleted: true},
			want: targetIn(model.CopyStateDeleted),
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := locked(targetIn(model.CopyStateCleanupPending))(mc)
				m.MarkDeletedMock.Expect(minimock.AnyContext, copyID).Return(targetIn(model.CopyStateDeleted), nil)
				return m
			},
		},
		{
			name: "failure: copy stays counted as cleanup_failed",
			args: args{deleted: false, errorMessage: "storage unavailable"},
			want: targetIn(model.CopyStateCleanupFailed),
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := locked(targetIn(model.CopyStateCleanupPending))(mc)
				m.MarkCleanupFailedMock.Expect(minimock.AnyContext, copyID, "storage unavailable").Return(targetIn(model.CopyStateCleanupFailed), nil)
				return m
			},
		},
		{
			name: "retry: failed cleanup can succeed later",
			args: args{deleted: true},
			want: targetIn(model.CopyStateDeleted),
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := locked(targetIn(model.CopyStateCleanupFailed))(mc)
				m.MarkDeletedMock.Return(targetIn(model.CopyStateDeleted), nil)
				return m
			},
		},
		{
			name:          "idempotent: repeated success on deleted copy",
			args:          args{deleted: true},
			want:          targetIn(model.CopyStateDeleted),
			buildCopyMock: locked(targetIn(model.CopyStateDeleted)),
		},
		{
			name:          "error: reserved target is not waiting for cleanup",
			args:          args{deleted: true},
			wantErr:       domainerrors.ErrInvalidCopyState,
			buildCopyMock: locked(reservedTarget()),
		},
		{
			name:          "error: failure reported for already deleted copy",
			args:          args{deleted: false, errorMessage: "late failure"},
			wantErr:       domainerrors.ErrInvalidCopyState,
			buildCopyMock: locked(targetIn(model.CopyStateDeleted)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)

			svc := placementService.NewService(mocks.NewPlacementRepositoryMock(mc), tt.buildCopyMock(mc), executingTx(mc))

			got, err := svc.CompleteCleanup(context.Background(), copyID, tt.args.deleted, tt.args.errorMessage)

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
		})
	}

	t.Run("validation: failure requires a message", func(t *testing.T) {
		mc := minimock.NewController(t)
		svc := placementService.NewService(mocks.NewPlacementRepositoryMock(mc), mocks.NewCopyRepositoryMock(mc), unusedTx(mc))

		_, err := svc.CompleteCleanup(context.Background(), copyID, false, "")

		requireValidationError(t, err)
	})
}
