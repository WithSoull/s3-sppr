package tests

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

const (
	migrationID = "4a9f6f62-6f0e-4c6b-9d55-0e0d8f4b6a01"
	versionID   = "1d3c2b7e-2f4e-4d8a-8f61-6c1a3e9b7c02"
	copyID      = "9b7c5e1a-3d2f-4a6b-8c9d-0e1f2a3b4c03"
	sourceBlob  = "6e5d4c3b-2a19-4f8e-9d7c-6b5a4f3e2d04"
	targetBlob  = "0f1e2d3c-4b5a-4968-8776-655443322105"
	validMD5    = "9e107d9d372bb6826bd81d3542a419d6"
)

func executingTx(mc *minimock.Controller) *mocks.TxManagerMock {
	m := mocks.NewTxManagerMock(mc)
	m.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error {
		return f(ctx)
	})
	return m
}

func unusedTx(mc *minimock.Controller) *mocks.TxManagerMock {
	return mocks.NewTxManagerMock(mc)
}

func coldPlacement() *model.Placement {
	return &model.Placement{
		VersionID:    versionID,
		ObjectActive: true,
		IsCurrent:    true,
		StorageClass: model.StorageClassCold,
		BlobID:       sourceBlob,
		Generation:   1,
		SizeBytes:    1024,
	}
}

func reservedTarget() *model.PlacementCopy {
	version := versionID
	generation := int64(1)
	expectedClass := model.StorageClassCold

	return &model.PlacementCopy{
		CopyID:             copyID,
		MigrationID:        migrationID,
		VersionID:          &version,
		Role:               model.CopyRoleTarget,
		StorageClass:       model.StorageClassHot,
		BlobID:             targetBlob,
		SizeBytes:          1024,
		ExpectedGeneration: &generation,
		ExpectedClass:      &expectedClass,
		State:              model.CopyStateReserved,
	}
}

func targetIn(state model.CopyState) *model.PlacementCopy {
	c := reservedTarget()
	c.State = state
	return c
}

func sourceCopy() *model.PlacementCopy {
	version := versionID
	cleanupAfter := time.Date(2026, 10, 26, 12, 0, 30, 0, time.UTC)

	return &model.PlacementCopy{
		CopyID:       "aa7c5e1a-3d2f-4a6b-8c9d-0e1f2a3b4c06",
		MigrationID:  migrationID,
		VersionID:    &version,
		Role:         model.CopyRoleSource,
		StorageClass: model.StorageClassCold,
		BlobID:       sourceBlob,
		SizeBytes:    1024,
		State:        model.CopyStateCleanupPending,
		CleanupAfter: &cleanupAfter,
	}
}

func requireValidationError(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	require.True(t, validate.IsValidationError(err))
}

func evictionPlacement() *model.Placement {
	p := coldPlacement()
	p.IsCurrent = false
	p.StorageClass = model.StorageClassHot
	return p
}

func evictionTarget() *model.PlacementCopy {
	c := reservedTarget()
	source := model.StorageClassHot
	c.ExpectedClass = &source
	c.StorageClass = model.StorageClassCold
	return c
}

func evictionSource() *model.PlacementCopy {
	c := sourceCopy()
	c.StorageClass = model.StorageClassHot
	return c
}
