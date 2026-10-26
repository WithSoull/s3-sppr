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

func TestAbortCopy(t *testing.T) {
	tests := []struct {
		name          string
		migrationID   string
		want          *model.PlacementCopy
		wantErr       error
		buildCopyMock func(mc *minimock.Controller) *mocks.CopyRepositoryMock
		buildTxMock   func(mc *minimock.Controller) *mocks.TxManagerMock
	}{
		{
			name:        "success: reserved target is scheduled for immediate cleanup",
			migrationID: migrationID,
			want:        targetIn(model.CopyStateCleanupPending),
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.LockByMigrationMock.Expect(minimock.AnyContext, migrationID, model.CopyRoleTarget).Return(reservedTarget(), nil)
				m.ScheduleCleanupMock.Expect(minimock.AnyContext, copyID, 0, "md5 mismatch").Return(targetIn(model.CopyStateCleanupPending), nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name:        "idempotent: already aborted copy is returned as is",
			migrationID: migrationID,
			want:        targetIn(model.CopyStateCleanupFailed),
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.LockByMigrationMock.Return(targetIn(model.CopyStateCleanupFailed), nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name:        "error: switched copy cannot be aborted",
			migrationID: migrationID,
			wantErr:     domainerrors.ErrInvalidCopyState,
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.LockByMigrationMock.Return(targetIn(model.CopyStatePromoted), nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name:        "error: unknown migration",
			migrationID: migrationID,
			wantErr:     domainerrors.ErrCopyNotFound,
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.LockByMigrationMock.Return(nil, domainerrors.ErrCopyNotFound)
				return m
			},
			buildTxMock: executingTx,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)

			svc := placementService.NewService(mocks.NewPlacementRepositoryMock(mc), tt.buildCopyMock(mc), tt.buildTxMock(mc))

			got, err := svc.AbortCopy(context.Background(), tt.migrationID, "md5 mismatch")

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
		})
	}

	t.Run("validation: migration_id must be a UUID", func(t *testing.T) {
		mc := minimock.NewController(t)
		svc := placementService.NewService(mocks.NewPlacementRepositoryMock(mc), mocks.NewCopyRepositoryMock(mc), unusedTx(mc))

		_, err := svc.AbortCopy(context.Background(), "m-1", "")

		requireValidationError(t, err)
	})
}
