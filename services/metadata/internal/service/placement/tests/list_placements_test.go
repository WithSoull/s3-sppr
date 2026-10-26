package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	placementService "github.com/alesplll/opens3-rebac/services/metadata/internal/service/placement"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestListPlacements(t *testing.T) {
	type args struct {
		pageSize  int32
		pageToken string
	}

	page := func(ids ...string) []*model.Placement {
		res := make([]*model.Placement, 0, len(ids))
		for _, id := range ids {
			res = append(res, &model.Placement{VersionID: id})
		}
		return res
	}

	tests := []struct {
		name               string
		args               args
		want               []*model.Placement
		wantToken          string
		wantErr            error
		buildPlacementMock func(mc *minimock.Controller) *mocks.PlacementRepositoryMock
	}{
		{
			name:      "more rows than page: token is the last returned version",
			args:      args{pageSize: 2},
			want:      page("v1", "v2"),
			wantToken: "v2",
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListCurrentMock.Expect(minimock.AnyContext, "", 3).Return(page("v1", "v2", "v3"), nil)
				return m
			},
		},
		{
			name: "exactly a full last page: no token",
			args: args{pageSize: 2, pageToken: versionID},
			want: page("v3", "v4"),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListCurrentMock.Expect(minimock.AnyContext, versionID, 3).Return(page("v3", "v4"), nil)
				return m
			},
		},
		{
			name: "page size is capped",
			args: args{pageSize: 5000},
			want: page(),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListCurrentMock.Expect(minimock.AnyContext, "", 1001).Return(page(), nil)
				return m
			},
		},
		{
			name:    "repository error",
			args:    args{pageSize: 10},
			wantErr: errors.New("db down"),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.ListCurrentMock.Return(nil, errors.New("db down"))
				return m
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)

			svc := placementService.NewService(tt.buildPlacementMock(mc), mocks.NewCopyRepositoryMock(mc), unusedTx(mc))

			got, token, err := svc.ListPlacements(context.Background(), tt.args.pageSize, tt.args.pageToken)

			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantToken, token)
		})
	}

	t.Run("validation: page token must be a version UUID", func(t *testing.T) {
		mc := minimock.NewController(t)
		svc := placementService.NewService(mocks.NewPlacementRepositoryMock(mc), mocks.NewCopyRepositoryMock(mc), unusedTx(mc))

		_, _, err := svc.ListPlacements(context.Background(), 10, "photos/cat.jpg")

		requireValidationError(t, err)
	})
}
