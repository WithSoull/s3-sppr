package tests

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	objectservice "github.com/alesplll/opens3-rebac/services/gateway/internal/service/object"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestPut(t *testing.T) {
	logger.SetNopLogger()
	type args struct {
		ctx         context.Context
		bucket, key string
		body        io.Reader
		size        *int64
		contentType string
	}
	type mockBuilder func(*minimock.Controller, args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock)
	upstreamErr := errors.New("upstream failed")
	size := int64(4)
	input := args{
		ctx:         context.Background(),
		bucket:      "bucket",
		key:         "nested/key",
		body:        strings.NewReader("data"),
		size:        &size,
		contentType: "text/plain",
	}
	stored := model.StoredObject{BlobID: "blob", Size: 4, ETag: "etag"}
	tests := []struct {
		name                            string
		args                            args
		want                            model.PutResult
		wantErr                         error
		wantStoreCalls, wantCreateCalls uint64
		wantAccessCalls                 uint64
		buildMocks                      mockBuilder
	}{
		{
			name:            "success",
			args:            input,
			wantStoreCalls:  1,
			wantCreateCalls: 1,
			wantAccessCalls: 1,
			want:            model.PutResult{ETag: "etag", VersionID: "version"},
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				storage.StoreMock.Expect(a.ctx, a.body, a.size, a.contentType).Return(stored, nil)
				meta.CreateVersionMock.
					ExpectBucketParam2(a.bucket).
					ExpectKeyParam3(a.key).
					ExpectBlobIDParam4(stored.BlobID).
					ExpectSizeParam5(stored.Size).
					ExpectEtagParam6(stored.ETag).
					ExpectContentTypeParam7(a.contentType).
					Return("version", nil)
				access.ProduceObjectWrittenMock.Expect(a.ctx, model.ObjectWrittenEvent{
					Bucket: a.bucket, ObjectKey: a.key, VersionID: "version", Bytes: stored.Size, Origin: model.OriginClient,
				}).Return()
				return meta, storage, access
			},
		},
		{
			name:    "bucket lookup error",
			args:    input,
			wantErr: upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(false, upstreamErr)
				return meta, storage, access
			},
		},
		{
			name:    "missing bucket",
			args:    input,
			wantErr: domainerrors.ErrBucketNotFound,
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(false, nil)
				return meta, storage, access
			},
		},
		{
			name:           "storage error",
			args:           input,
			wantStoreCalls: 1,
			wantErr:        upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				storage.StoreMock.Expect(a.ctx, a.body, a.size, a.contentType).Return(model.StoredObject{}, upstreamErr)
				return meta, storage, access
			},
		},
		{
			name:            "metadata registration error",
			args:            input,
			wantStoreCalls:  1,
			wantCreateCalls: 1,
			wantErr:         upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				storage.StoreMock.Expect(a.ctx, a.body, a.size, a.contentType).Return(stored, nil)
				meta.CreateVersionMock.
					ExpectBucketParam2(a.bucket).
					ExpectKeyParam3(a.key).
					ExpectBlobIDParam4(stored.BlobID).
					ExpectSizeParam5(stored.Size).
					ExpectEtagParam6(stored.ETag).
					ExpectContentTypeParam7(a.contentType).
					Return("", upstreamErr)
				return meta, storage, access
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			meta, storage, access := tt.buildMocks(mc, tt.args)
			result, err := objectservice.NewService(meta, storage, access).Put(tt.args.ctx, tt.args.bucket, tt.args.key, tt.args.body, tt.args.size, tt.args.contentType)
			require.Equal(t, tt.want, result)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantStoreCalls, storage.StoreAfterCounter())
			require.Equal(t, tt.wantCreateCalls, meta.CreateVersionAfterCounter())
			require.Equal(t, tt.wantAccessCalls, access.ProduceObjectWrittenAfterCounter(), "only a registered version is published")
		})
	}
}

func TestPutRegistersVersionAfterRequestCancel(t *testing.T) {
	logger.SetNopLogger()
	mc := minimock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	size := int64(4)
	stored := model.StoredObject{BlobID: "blob", Size: 4, ETag: "etag"}
	meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
	meta.HeadBucketMock.Expect(ctx, "bucket").Return(true, nil)
	storage.StoreMock.Set(func(context.Context, io.Reader, *int64, string) (model.StoredObject, error) {
		cancel()
		return stored, nil
	})
	meta.CreateVersionMock.Set(func(ctx context.Context, _, _, blobID string, _ int64, _, _ string) (string, error) {
		require.NoError(t, ctx.Err())
		require.Equal(t, "blob", blobID)
		return "version", nil
	})
	access.ProduceObjectWrittenMock.Return()
	result, err := objectservice.NewService(meta, storage, access).Put(ctx, "bucket", "key", strings.NewReader("data"), &size, "text/plain")
	require.NoError(t, err)
	require.Equal(t, model.PutResult{ETag: "etag", VersionID: "version"}, result)
}
