package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	placementhandler "github.com/alesplll/opens3-rebac/services/metadata/internal/handler/placement"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestSwitchPlacement(t *testing.T) {
	t.Run("success: converts delay and source copy", func(t *testing.T) {
		ctx := context.Background()
		mc := minimock.NewController(t)
		versionID := "version-1"
		cleanupAfter := time.UnixMilli(1761480030000)
		createdAt := time.UnixMilli(1761480000000)

		svc := mocks.NewPlacementServiceMock(mc)
		svc.SwitchPlacementMock.
			Expect(ctx, "migration-1", int64(1024), "9e107d9d372bb6826bd81d3542a419d6", 30*time.Second).
			Return(&model.SwitchResult{
				VersionID:     versionID,
				NewGeneration: 2,
				StorageClass:  model.StorageClassHot,
				BlobID:        "blob-hot",
				SourceCopy: &model.PlacementCopy{
					CopyID:       "copy-src",
					MigrationID:  "migration-1",
					VersionID:    &versionID,
					Role:         model.CopyRoleSource,
					StorageClass: model.StorageClassCold,
					BlobID:       "blob-cold",
					SizeBytes:    1024,
					State:        model.CopyStateCleanupPending,
					CleanupAfter: &cleanupAfter,
					CreatedAt:    createdAt,
					UpdatedAt:    createdAt,
				},
			}, nil)

		res, err := placementhandler.NewHandler(svc).SwitchPlacement(ctx, &metadatav1.SwitchPlacementRequest{
			MigrationId:          "migration-1",
			VerifiedSizeBytes:    1024,
			VerifiedMd5:          "9e107d9d372bb6826bd81d3542a419d6",
			SourceCleanupDelayMs: 30_000,
		})

		require.NoError(t, err)
		require.Equal(t, &metadatav1.SwitchPlacementResponse{
			VersionId:     versionID,
			NewGeneration: 2,
			StorageClass:  "HOT",
			BlobId:        "blob-hot",
			SourceCopy: &metadatav1.PlacementCopyInfo{
				CopyId:       "copy-src",
				MigrationId:  "migration-1",
				VersionId:    versionID,
				Role:         "source",
				StorageClass: "COLD",
				BlobId:       "blob-cold",
				SizeBytes:    1024,
				State:        "cleanup_pending",
				CleanupAfter: cleanupAfter.UnixMilli(),
				CreatedAt:    createdAt.UnixMilli(),
				UpdatedAt:    createdAt.UnixMilli(),
			},
		}, res)
	})

	t.Run("service error", func(t *testing.T) {
		ctx := context.Background()
		mc := minimock.NewController(t)
		serviceErr := errors.New("service error")

		svc := mocks.NewPlacementServiceMock(mc)
		svc.SwitchPlacementMock.Return(nil, serviceErr)

		res, err := placementhandler.NewHandler(svc).SwitchPlacement(ctx, &metadatav1.SwitchPlacementRequest{MigrationId: "migration-1"})

		require.Nil(t, res)
		require.ErrorIs(t, err, serviceErr)
	})
}
