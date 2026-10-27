package tests

import (
	"context"
	"errors"
	"testing"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	objectService "github.com/alesplll/opens3-rebac/services/metadata/internal/service/object"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestChooseWriteClass(t *testing.T) {
	type args struct {
		bucketName string
		key        string
		sizeBytes  *int64
	}

	ctx := context.Background()
	size := func(n int64) *int64 { return &n }
	repoErr := errors.New("db error")

	// usable 1000 - placed 600 - copies 100 = 300 free
	hotUsage := &model.StorageClassUsage{
		Name:        model.StorageClassHot,
		UsableBytes: 1000,
		PlacedBytes: 600,
		CopyBytes:   100,
	}
	currentIn := func(class model.StorageClass) func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
		return func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
			m := mocks.NewObjectRepositoryMock(mc)
			m.GetMetaMock.Expect(minimock.AnyContext, "my-bucket", "report.csv", "").
				Return(&model.ObjectMeta{VersionID: "version-1", StorageClass: class}, nil)
			return m
		}
	}

	tests := []struct {
		name                string
		args                args
		want                *model.WriteClassDecision
		wantErr             error
		wantValidationError bool
		buildRepoMock       func(mc *minimock.Controller) *mocks.ObjectRepositoryMock
		buildPlacementMock  func(mc *minimock.Controller) *mocks.PlacementRepositoryMock
	}{
		{
			name: "new key goes to COLD",
			args: args{bucketName: "my-bucket", key: "report.csv", sizeBytes: size(10)},
			want: &model.WriteClassDecision{StorageClass: model.StorageClassCold, Reason: model.WriteClassReasonNewKey},
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				m := mocks.NewObjectRepositoryMock(mc)
				m.GetMetaMock.Return(nil, domainerrors.ErrObjectNotFound)
				return m
			},
		},
		{
			name:          "overwrite of COLD key stays in COLD",
			args:          args{bucketName: "my-bucket", key: "report.csv", sizeBytes: size(10)},
			want:          &model.WriteClassDecision{StorageClass: model.StorageClassCold, Reason: model.WriteClassReasonPreviousCold},
			buildRepoMock: currentIn(model.StorageClassCold),
		},
		{
			name:          "overwrite of HOT key inherits HOT when it fits",
			args:          args{bucketName: "my-bucket", key: "report.csv", sizeBytes: size(300)},
			want:          &model.WriteClassDecision{StorageClass: model.StorageClassHot, Reason: model.WriteClassReasonInherited},
			buildRepoMock: currentIn(model.StorageClassHot),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.GetUsageMock.Expect(minimock.AnyContext, model.StorageClassHot).Return(hotUsage, nil)
				return m
			},
		},
		{
			name:          "overwrite of HOT key goes to COLD when HOT has no room",
			args:          args{bucketName: "my-bucket", key: "report.csv", sizeBytes: size(301)},
			want:          &model.WriteClassDecision{StorageClass: model.StorageClassCold, Reason: model.WriteClassReasonHotFull},
			buildRepoMock: currentIn(model.StorageClassHot),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.GetUsageMock.Return(hotUsage, nil)
				return m
			},
		},
		{
			name:          "overwrite of HOT key without size goes to COLD",
			args:          args{bucketName: "my-bucket", key: "report.csv"},
			want:          &model.WriteClassDecision{StorageClass: model.StorageClassCold, Reason: model.WriteClassReasonSizeUnknown},
			buildRepoMock: currentIn(model.StorageClassHot),
		},
		{
			name:    "error: GetMeta fails",
			args:    args{bucketName: "my-bucket", key: "report.csv", sizeBytes: size(10)},
			wantErr: repoErr,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				m := mocks.NewObjectRepositoryMock(mc)
				m.GetMetaMock.Return(nil, repoErr)
				return m
			},
		},
		{
			name:          "error: GetUsage fails",
			args:          args{bucketName: "my-bucket", key: "report.csv", sizeBytes: size(10)},
			wantErr:       repoErr,
			buildRepoMock: currentIn(model.StorageClassHot),
			buildPlacementMock: func(mc *minimock.Controller) *mocks.PlacementRepositoryMock {
				m := mocks.NewPlacementRepositoryMock(mc)
				m.GetUsageMock.Return(nil, repoErr)
				return m
			},
		},
		{
			name:                "validation error: empty key",
			args:                args{bucketName: "my-bucket", key: "", sizeBytes: size(10)},
			wantValidationError: true,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				return mocks.NewObjectRepositoryMock(mc)
			},
		},
		{
			name:                "validation error: negative size",
			args:                args{bucketName: "my-bucket", key: "report.csv", sizeBytes: size(-1)},
			wantValidationError: true,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				return mocks.NewObjectRepositoryMock(mc)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)

			repoMock := tt.buildRepoMock(mc)
			placementMock := mocks.NewPlacementRepositoryMock(mc)
			if tt.buildPlacementMock != nil {
				placementMock = tt.buildPlacementMock(mc)
			}

			svc := objectService.NewService(repoMock, placementMock, mocks.NewTxManagerMock(mc), mocks.NewProducerMock(mc), nil, nil)

			got, err := svc.ChooseWriteClass(ctx, tt.args.bucketName, tt.args.key, tt.args.sizeBytes)

			if tt.wantValidationError {
				requireValidationError(t, err)
				require.Equal(t, uint64(0), repoMock.GetMetaBeforeCounter())
				return
			}

			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, got)
		})
	}
}
