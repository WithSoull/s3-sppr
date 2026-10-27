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
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	type args struct {
		ctx         context.Context
		bucket, key string
	}
	type mockBuilder func(*minimock.Controller, args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock)
	upstreamErr := errors.New("upstream failed")
	input := args{ctx: context.Background(), bucket: "bucket", key: "nested/key"}
	metaResult := model.ObjectMeta{BlobID: "blob", VersionID: "version", Size: 4, ETag: "etag", ContentType: "text/plain"}
	body := io.NopCloser(strings.NewReader("data"))
	tests := []struct {
		name              string
		args              args
		want              model.GetResult
		wantErr           error
		wantRetrieveCalls uint64
		wantAccessCalls   uint64
		buildMocks        mockBuilder
	}{
		{
			name:              "success",
			args:              input,
			wantRetrieveCalls: 1,
			wantAccessCalls:   1,
			want:              model.GetResult{Body: body, Size: 4, ETag: "etag", VersionID: "version", ContentType: "text/plain"},
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(metaResult, nil)
				storage.RetrieveMock.Expect(a.ctx, metaResult.BlobID, metaResult.Size).Return(body, nil)
				access.ProduceObjectReadMock.Expect(a.ctx, model.ObjectReadEvent{
					Bucket: a.bucket, ObjectKey: a.key, VersionID: "version", Bytes: 4, Origin: model.OriginClient,
				}).Return()
				return meta, storage, access
			},
		},
		{
			name:    "metadata error",
			args:    input,
			wantErr: upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(model.ObjectMeta{}, upstreamErr)
				return meta, storage, access
			},
		},
		{
			name:              "storage error",
			args:              input,
			wantRetrieveCalls: 1,
			wantErr:           upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(metaResult, nil)
				storage.RetrieveMock.Expect(a.ctx, metaResult.BlobID, metaResult.Size).Return(nil, upstreamErr)
				return meta, storage, access
			},
		},
		{
			name:              "empty storage stream",
			args:              input,
			wantRetrieveCalls: 1,
			wantErr:           domainerrors.ErrEmptyStorageStream,
			buildMocks: func(mc *minimock.Controller, a args) (*mocks.MetadataClientMock, *mocks.StorageClientMock, *mocks.AccessProducerServiceMock) {
				meta, storage, access := mocks.NewMetadataClientMock(mc), mocks.NewStorageClientMock(mc), mocks.NewAccessProducerServiceMock(mc)
				meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(metaResult, nil)
				storage.RetrieveMock.Expect(a.ctx, metaResult.BlobID, metaResult.Size).Return(nil, domainerrors.ErrEmptyStorageStream)
				return meta, storage, access
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			meta, storage, access := tt.buildMocks(mc, tt.args)
			result, err := objectservice.NewService(meta, storage, access).Get(tt.args.ctx, tt.args.bucket, tt.args.key)
			require.Equal(t, tt.want, result)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantRetrieveCalls, storage.RetrieveAfterCounter())
			require.Equal(t, tt.wantAccessCalls, access.ProduceObjectReadAfterCounter(), "only a successful Get is published")
			if result.Body != nil {
				require.NoError(t, result.Body.Close())
			}
		})
	}
}
