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

func TestListPlacements(t *testing.T) {
	ctx := context.Background()
	createdAt := time.UnixMilli(1761480000123)
	updatedAt := createdAt.Add(time.Hour)
	current := &model.Placement{
		VersionID: "current", ObjectID: "object", BucketName: "bucket", Key: "key",
		IsCurrent: true, StorageClass: model.StorageClassCold, BlobID: "cold-blob",
		Generation: 3, SizeBytes: 1024, UpdatedAt: updatedAt, VersionCreatedAt: createdAt,
		MigrationInProgress: true,
	}
	old := &model.Placement{
		VersionID: "old", ObjectID: "object", BucketName: "bucket", Key: "key",
		StorageClass: model.StorageClassHot, BlobID: "hot-blob", Generation: 1,
		SizeBytes: 2048, UpdatedAt: updatedAt, VersionCreatedAt: createdAt.Add(-time.Hour),
	}
	currentProto := &metadatav1.PlacementInfo{
		VersionId: "current", ObjectId: "object", BucketName: "bucket", Key: "key",
		IsCurrent: true, StorageClass: "COLD", BlobId: "cold-blob", Generation: 3,
		SizeBytes: 1024, UpdatedAt: updatedAt.UnixMilli(), VersionCreatedAt: createdAt.UnixMilli(),
		MigrationInProgress: true,
	}
	oldProto := &metadatav1.PlacementInfo{
		VersionId: "old", ObjectId: "object", BucketName: "bucket", Key: "key",
		StorageClass: "HOT", BlobId: "hot-blob", Generation: 1, SizeBytes: 2048,
		UpdatedAt: updatedAt.UnixMilli(), VersionCreatedAt: createdAt.Add(-time.Hour).UnixMilli(),
	}
	serviceErr := errors.New("catalog unavailable")
	tests := []struct {
		name             string
		args             *metadatav1.ListPlacementsRequest
		want             *metadatav1.ListPlacementsResponse
		wantErr          error
		buildServiceMock func(*minimock.Controller) *mocks.PlacementServiceMock
	}{
		{
			name: "default flag and current version creation time",
			args: &metadatav1.ListPlacementsRequest{PageSize: 2, PageToken: "previous"},
			want: &metadatav1.ListPlacementsResponse{Placements: []*metadatav1.PlacementInfo{currentProto}, NextPageToken: "current"},
			buildServiceMock: func(mc *minimock.Controller) *mocks.PlacementServiceMock {
				m := mocks.NewPlacementServiceMock(mc)
				m.ListPlacementsMock.Expect(ctx, model.ListPlacementsRequest{PageSize: 2, PageToken: "previous"}).Return([]*model.Placement{current}, "current", nil)
				return m
			},
		},
		{
			name: "include noncurrent HOT flag and creation time of every row",
			args: &metadatav1.ListPlacementsRequest{PageSize: 10, IncludeNoncurrentHot: true},
			want: &metadatav1.ListPlacementsResponse{Placements: []*metadatav1.PlacementInfo{currentProto, oldProto}},
			buildServiceMock: func(mc *minimock.Controller) *mocks.PlacementServiceMock {
				m := mocks.NewPlacementServiceMock(mc)
				m.ListPlacementsMock.Expect(ctx, model.ListPlacementsRequest{PageSize: 10, IncludeNoncurrentHot: true}).Return([]*model.Placement{current, old}, "", nil)
				return m
			},
		},
		{
			name: "service error", args: &metadatav1.ListPlacementsRequest{IncludeNoncurrentHot: true}, wantErr: serviceErr,
			buildServiceMock: func(mc *minimock.Controller) *mocks.PlacementServiceMock {
				m := mocks.NewPlacementServiceMock(mc)
				m.ListPlacementsMock.Expect(ctx, model.ListPlacementsRequest{IncludeNoncurrentHot: true}).Return(nil, "", serviceErr)
				return m
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			got, err := placementhandler.NewHandler(tt.buildServiceMock(mc)).ListPlacements(ctx, tt.args)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, got)
		})
	}
}
