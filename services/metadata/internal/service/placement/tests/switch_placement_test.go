package tests

import (
	"context"
	"testing"
	"time"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	placementService "github.com/alesplll/opens3-rebac/services/metadata/internal/service/placement"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestSwitchPlacement(t *testing.T) {
	type args struct {
		migrationID  string
		verifiedSize int64
		verifiedMD5  string
		cleanupDelay time.Duration
	}

	validArgs := args{migrationID: migrationID, verifiedSize: 1024, verifiedMD5: validMD5, cleanupDelay: 30 * time.Second}

	lockedTarget := func(target *model.PlacementCopy) func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
		return func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
			m := mocks.NewCopyRepositoryMock(mc)
			m.LockByMigrationMock.Expect(minimock.AnyContext, migrationID, model.CopyRoleTarget).Return(target, nil)
			return m
		}
	}
	untouchedPlacement := func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
		return mocks.NewPlacementRepositoryMock(mc)
	}

	tests := []struct {
		name                string
		args                args
		want                *model.SwitchResult
		wantErr             error
		wantValidationError bool
		buildPlacementMock  func(mc *minimock.Controller) *mocks.PlacementRepositoryMock
		buildCopyMock       func(mc *minimock.Controller) *mocks.CopyRepositoryMock
		buildTxMock         func(mc *minimock.Controller) *mocks.TxManagerMock
	}{
		{
			name: "success: CAS moves placement and keeps old blob for cleanup",
			args: validArgs,
			want: &model.SwitchResult{
				VersionID:     versionID,
				NewGeneration: 2,
				StorageClass:  model.StorageClassHot,
				BlobID:        targetBlob,
				SourceCopy:    sourceCopy(),
			},
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Expect(minimock.AnyContext, versionID).Return(coldPlacement(), nil)
				m.CompareAndSwapMock.
					Expect(minimock.AnyContext, versionID, int64(1), model.StorageClassCold, model.StorageClassHot, targetBlob).
					Return(2, nil)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := lockedTarget(reservedTarget())(mc)
				m.InsertSourceMock.Expect(minimock.AnyContext, migrationID, coldPlacement(), 30*time.Second).Return(sourceCopy(), nil)
				m.PromoteMock.Expect(minimock.AnyContext, copyID, validMD5).Return(nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name: "retry after commit: returns previous result without a second CAS",
			args: validArgs,
			want: &model.SwitchResult{
				VersionID:     versionID,
				NewGeneration: 2,
				StorageClass:  model.StorageClassHot,
				BlobID:        targetBlob,
				SourceCopy:    sourceCopy(),
			},
			buildPlacementMock: untouchedPlacement,
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := lockedTarget(targetIn(model.CopyStatePromoted))(mc)
				m.GetByMigrationMock.Expect(minimock.AnyContext, migrationID, model.CopyRoleSource).Return(sourceCopy(), nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name:               "error: aborted copy cannot be switched",
			args:               validArgs,
			wantErr:            domainerrors.ErrInvalidCopyState,
			buildPlacementMock: untouchedPlacement,
			buildCopyMock:      lockedTarget(targetIn(model.CopyStateCleanupPending)),
			buildTxMock:        executingTx,
		},
		{
			name:               "error: verified size differs from version size",
			args:               args{migrationID: migrationID, verifiedSize: 1000, verifiedMD5: validMD5, cleanupDelay: time.Second},
			wantErr:            domainerrors.ErrCopySizeMismatch,
			buildPlacementMock: untouchedPlacement,
			buildCopyMock:      lockedTarget(reservedTarget()),
			buildTxMock:        executingTx,
		},
		{
			name:    "error: object overwritten after reservation",
			args:    validArgs,
			wantErr: domainerrors.ErrVersionNotCurrent,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				p := coldPlacement()
				p.IsCurrent = false
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Return(p, nil)
				return m
			},
			buildCopyMock: lockedTarget(reservedTarget()),
			buildTxMock:   executingTx,
		},
		{
			name:    "error: object deleted after reservation",
			args:    validArgs,
			wantErr: domainerrors.ErrVersionNotCurrent,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Return(nil, domainerrors.ErrPlacementNotFound)
				return m
			},
			buildCopyMock: lockedTarget(reservedTarget()),
			buildTxMock:   executingTx,
		},
		{
			name:    "error: CAS conflict leaves placement and copies unchanged",
			args:    validArgs,
			wantErr: domainerrors.ErrPlacementChanged,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Return(coldPlacement(), nil)
				m.CompareAndSwapMock.Return(0, domainerrors.ErrPlacementChanged)
				return m
			},
			buildCopyMock: lockedTarget(reservedTarget()),
			buildTxMock:   executingTx,
		},
		{
			name:                "validation: bad md5 and zero cleanup delay",
			args:                args{migrationID: migrationID, verifiedSize: 1024, verifiedMD5: "9E107D9D", cleanupDelay: 0},
			wantValidationError: true,
			buildPlacementMock:  untouchedPlacement,
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				return mocks.NewCopyRepositoryMock(mc)
			},
			buildTxMock: unusedTx,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)

			svc := placementService.NewService(tt.buildPlacementMock(mc), tt.buildCopyMock(mc), tt.buildTxMock(mc))

			got, err := svc.SwitchPlacement(context.Background(), tt.args.migrationID, tt.args.verifiedSize, tt.args.verifiedMD5, tt.args.cleanupDelay)

			if tt.wantValidationError {
				requireValidationError(t, err)
				return
			}

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
		})
	}
}
