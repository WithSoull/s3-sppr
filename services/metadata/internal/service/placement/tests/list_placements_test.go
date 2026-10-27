package tests

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	placementService "github.com/alesplll/opens3-rebac/services/metadata/internal/service/placement"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestListPlacements(t *testing.T) {
	page := func(ids ...string) []*model.Placement {
		res := make([]*model.Placement, 0, len(ids))
		for _, id := range ids {
			res = append(res, &model.Placement{VersionID: id, VersionCreatedAt: time.UnixMilli(1761480000123)})
		}
		return res
	}
	dbErr := errors.New("db down")
	tests := []struct {
		name               string
		args               model.ListPlacementsRequest
		want               []*model.Placement
		wantToken          string
		wantErr            error
		buildPlacementMock func(mc *minimock.Controller) *mocks.PlacementRepositoryMock
	}{
		{
			name: "more rows than page: token is the last returned version",
			args: model.ListPlacementsRequest{PageSize: 2},
			want: page("v1", "v2"), wantToken: "v2",
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListMock.Expect(minimock.AnyContext, model.ListPlacementsRequest{PageSize: 3}).Return(page("v1", "v2", "v3"), nil)
				return m
			},
		},
		{
			name: "exactly a full last page: no token",
			args: model.ListPlacementsRequest{PageSize: 2, PageToken: versionID}, want: page("v3", "v4"),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListMock.Expect(minimock.AnyContext, model.ListPlacementsRequest{PageSize: 3, PageToken: versionID}).Return(page("v3", "v4"), nil)
				return m
			},
		},
		{
			name: "page size is capped",
			args: model.ListPlacementsRequest{PageSize: 5000}, want: page(),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListMock.Expect(minimock.AnyContext, model.ListPlacementsRequest{PageSize: 1001}).Return(page(), nil)
				return m
			},
		},
		{
			name: "default page size preserves noncurrent flag",
			args: model.ListPlacementsRequest{IncludeNoncurrentHot: true}, want: page("old-hot", "current"),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListMock.Expect(minimock.AnyContext, model.ListPlacementsRequest{PageSize: 1001, IncludeNoncurrentHot: true}).Return(page("old-hot", "current"), nil)
				return m
			},
		},
		{
			name: "noncurrent flag and token pass to repository",
			args: model.ListPlacementsRequest{PageSize: 1, PageToken: versionID, IncludeNoncurrentHot: true},
			want: page("old-hot"), wantToken: "old-hot",
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListMock.Expect(minimock.AnyContext, model.ListPlacementsRequest{PageSize: 2, PageToken: versionID, IncludeNoncurrentHot: true}).Return(page("old-hot", "current"), nil)
				return m
			},
		},
		{
			name: "repository error",
			args: model.ListPlacementsRequest{PageSize: 10}, wantErr: dbErr,
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListMock.Expect(minimock.AnyContext, model.ListPlacementsRequest{PageSize: 11}).Return(nil, dbErr)
				return m
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			svc := placementService.NewService(tt.buildPlacementMock(mc), mocks.NewCopyRepositoryMock(mc), unusedTx(mc))
			got, token, err := svc.ListPlacements(context.Background(), tt.args)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantToken, token)
		})
	}
	for _, include := range []bool{false, true} {
		t.Run("invalid page token with include_noncurrent_hot="+strconv.FormatBool(include), func(t *testing.T) {
			mc := minimock.NewController(t)
			repo := mocks.NewPlacementRepositoryMock(mc)
			svc := placementService.NewService(repo, mocks.NewCopyRepositoryMock(mc), unusedTx(mc))
			_, _, err := svc.ListPlacements(context.Background(), model.ListPlacementsRequest{PageSize: 10, PageToken: "photos/cat.jpg", IncludeNoncurrentHot: include})
			requireValidationError(t, err)
			require.Zero(t, repo.ListAfterCounter())
		})
	}
}
