package tests

import (
	"context"
	"testing"
	"time"

	placementhandler "github.com/alesplll/opens3-rebac/services/metadata/internal/handler/placement"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestReserveCopy(t *testing.T) {
	ctx := context.Background()
	mc := minimock.NewController(t)
	createdAt := time.UnixMilli(1761480000000)
	generation := int64(1)
	expectedClass := model.StorageClassCold

	svc := mocks.NewPlacementServiceMock(mc)
	svc.ReserveCopyMock.
		Expect(ctx, "migration-1", "version-1", int64(1), model.StorageClassCold, model.StorageClassHot).
		Return(&model.PlacementCopy{
			CopyID:             "copy-1",
			MigrationID:        "migration-1",
			Role:               model.CopyRoleTarget,
			StorageClass:       model.StorageClassHot,
			BlobID:             "blob-hot",
			SizeBytes:          1024,
			ExpectedGeneration: &generation,
			ExpectedClass:      &expectedClass,
			State:              model.CopyStateReserved,
			CreatedAt:          createdAt,
			UpdatedAt:          createdAt,
		}, nil)

	res, err := placementhandler.NewHandler(svc).ReserveCopy(ctx, &metadatav1.ReserveCopyRequest{
		MigrationId:        "migration-1",
		VersionId:          "version-1",
		ExpectedGeneration: 1,
		SourceClass:        "COLD",
		TargetClass:        "HOT",
	})

	require.NoError(t, err)
	// VersionID is nil (version deleted) and CleanupAfter is unset: both map to zero values.
	require.Equal(t, &metadatav1.ReserveCopyResponse{Copy: &metadatav1.PlacementCopyInfo{
		CopyId:             "copy-1",
		MigrationId:        "migration-1",
		Role:               "target",
		StorageClass:       "HOT",
		BlobId:             "blob-hot",
		SizeBytes:          1024,
		ExpectedGeneration: 1,
		ExpectedClass:      "COLD",
		State:              "reserved",
		CreatedAt:          createdAt.UnixMilli(),
		UpdatedAt:          createdAt.UnixMilli(),
	}}, res)
}
