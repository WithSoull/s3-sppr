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

func TestReserveCopy(t *testing.T) {
	type args struct {
		migrationID        string
		versionID          string
		expectedGeneration int64
		sourceClass        model.StorageClass
		targetClass        model.StorageClass
	}

	validArgs := args{
		migrationID:        migrationID,
		versionID:          versionID,
		expectedGeneration: 1,
		sourceClass:        model.StorageClassCold,
		targetClass:        model.StorageClassHot,
	}

	hotUsage := func(free int64) *model.StorageClassUsage {
		return &model.StorageClassUsage{Name: model.StorageClassHot, UsableBytes: 10_000, PlacedBytes: 10_000 - free}
	}

	noCopy := func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
		m := mocks.NewCopyRepositoryMock(mc)
		m.GetByMigrationMock.Expect(minimock.AnyContext, migrationID, model.CopyRoleTarget).Return(nil, domainerrors.ErrCopyNotFound)
		return m
	}

	tests := []struct {
		name                string
		args                args
		want                *model.PlacementCopy
		wantErr             error
		wantValidationError bool
		buildPlacementMock  func(mc *minimock.Controller) *mocks.PlacementRepositoryMock
		buildCopyMock       func(mc *minimock.Controller) *mocks.CopyRepositoryMock
		buildTxMock         func(mc *minimock.Controller) *mocks.TxManagerMock
	}{
		{
			name: "success: reserves target in HOT",
			args: validArgs,
			want: reservedTarget(),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Expect(minimock.AnyContext, versionID).Return(coldPlacement(), nil)
				m.LockStorageClassMock.Expect(minimock.AnyContext, model.StorageClassHot).Return(nil)
				m.GetUsageMock.Expect(minimock.AnyContext, model.StorageClassHot).Return(hotUsage(1024), nil)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := noCopy(mc)
				m.InsertTargetMock.Expect(minimock.AnyContext, migrationID, coldPlacement(), model.StorageClassHot).Return(reservedTarget(), nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name: "success: non-current HOT version can be evicted to COLD",
			args: args{migrationID: migrationID, versionID: versionID, expectedGeneration: 1, sourceClass: model.StorageClassHot, targetClass: model.StorageClassCold},
			want: evictionTarget(),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Expect(minimock.AnyContext, versionID).Return(evictionPlacement(), nil)
				m.LockStorageClassMock.Expect(minimock.AnyContext, model.StorageClassCold).Return(nil)
				m.GetUsageMock.Expect(minimock.AnyContext, model.StorageClassCold).Return(&model.StorageClassUsage{UsableBytes: 1024}, nil)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := noCopy(mc)
				m.InsertTargetMock.Expect(minimock.AnyContext, migrationID, evictionPlacement(), model.StorageClassCold).Return(evictionTarget(), nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name:    "error: inactive object's HOT version cannot be evicted",
			args:    args{migrationID: migrationID, versionID: versionID, expectedGeneration: 1, sourceClass: model.StorageClassHot, targetClass: model.StorageClassCold},
			wantErr: domainerrors.ErrVersionNotCurrent,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				p := evictionPlacement()
				p.ObjectActive = false
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Expect(minimock.AnyContext, versionID).Return(p, nil)
				return m
			},
			buildCopyMock: noCopy,
			buildTxMock:   executingTx,
		},
		{
			name:    "error: eviction still checks generation",
			args:    args{migrationID: migrationID, versionID: versionID, expectedGeneration: 2, sourceClass: model.StorageClassHot, targetClass: model.StorageClassCold},
			wantErr: domainerrors.ErrPlacementChanged,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Expect(minimock.AnyContext, versionID).Return(evictionPlacement(), nil)
				return m
			},
			buildCopyMock: noCopy,
			buildTxMock:   executingTx,
		},
		{
			name: "retry: returns copy reserved by the same migration",
			args: validArgs,
			want: targetIn(model.CopyStateCleanupPending),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				return mocks.NewPlacementRepositoryMock(mc)
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.GetByMigrationMock.Return(targetIn(model.CopyStateCleanupPending), nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name:    "error: migration_id reused for another version",
			args:    args{migrationID: migrationID, versionID: sourceBlob, expectedGeneration: 1, sourceClass: model.StorageClassCold, targetClass: model.StorageClassHot},
			wantErr: domainerrors.ErrMigrationMismatch,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				return mocks.NewPlacementRepositoryMock(mc)
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.GetByMigrationMock.Return(reservedTarget(), nil)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name:    "error: version is not current",
			args:    validArgs,
			wantErr: domainerrors.ErrVersionNotCurrent,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				p := coldPlacement()
				p.IsCurrent = false
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Return(p, nil)
				return m
			},
			buildCopyMock: noCopy,
			buildTxMock:   executingTx,
		},
		{
			name:    "error: generation changed since the plan",
			args:    validArgs,
			wantErr: domainerrors.ErrPlacementChanged,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				p := coldPlacement()
				p.Generation = 2
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Return(p, nil)
				return m
			},
			buildCopyMock: noCopy,
			buildTxMock:   executingTx,
		},
		{
			name:    "error: version already in another class",
			args:    validArgs,
			wantErr: domainerrors.ErrPlacementChanged,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				p := coldPlacement()
				p.StorageClass = model.StorageClassHot
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Return(p, nil)
				return m
			},
			buildCopyMock: noCopy,
			buildTxMock:   executingTx,
		},
		{
			name:    "error: target class has less free space than the version",
			args:    validArgs,
			wantErr: domainerrors.ErrInsufficientCapacity,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Return(coldPlacement(), nil)
				m.LockStorageClassMock.Return(nil)
				m.GetUsageMock.Return(hotUsage(1023), nil)
				return m
			},
			buildCopyMock: noCopy,
			buildTxMock:   executingTx,
		},
		{
			name:    "error: another migration holds a reserved copy",
			args:    validArgs,
			wantErr: domainerrors.ErrCopyAlreadyReserved,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.LockForChangeMock.Return(coldPlacement(), nil)
				m.LockStorageClassMock.Return(nil)
				m.GetUsageMock.Return(hotUsage(4096), nil)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := noCopy(mc)
				m.InsertTargetMock.Return(nil, domainerrors.ErrCopyAlreadyReserved)
				return m
			},
			buildTxMock: executingTx,
		},
		{
			name:                "validation: same source and target class",
			args:                args{migrationID: migrationID, versionID: versionID, expectedGeneration: 1, sourceClass: model.StorageClassCold, targetClass: model.StorageClassCold},
			wantValidationError: true,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				return mocks.NewPlacementRepositoryMock(mc)
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				return mocks.NewCopyRepositoryMock(mc)
			},
			buildTxMock: unusedTx,
		},
		{
			name:                "validation: unknown class, bad ids and generation",
			args:                args{migrationID: "m-1", versionID: "", expectedGeneration: 0, sourceClass: "WARM", targetClass: model.StorageClassHot},
			wantValidationError: true,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				return mocks.NewPlacementRepositoryMock(mc)
			},
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

			got, err := svc.ReserveCopy(context.Background(), tt.args.migrationID, tt.args.versionID, tt.args.expectedGeneration, tt.args.sourceClass, tt.args.targetClass)

			if tt.wantValidationError {
				requireValidationError(t, err)
				return
			}

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
		})
	}
}
