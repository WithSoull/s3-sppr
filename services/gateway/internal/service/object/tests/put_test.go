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
	expectCreate := func(m upstreamMocks, a args, class model.StorageClass, versionID string, err error) {
		m.meta.CreateVersionMock.
			ExpectBucketParam2(a.bucket).
			ExpectKeyParam3(a.key).
			ExpectBlobIDParam4(stored.BlobID).
			ExpectSizeParam5(stored.Size).
			ExpectEtagParam6(stored.ETag).
			ExpectContentTypeParam7(a.contentType).
			ExpectClassParam8(class).
			Return(versionID, err)
	}
	written := model.ObjectWrittenEvent{Bucket: "bucket", ObjectKey: "nested/key", VersionID: "version", Bytes: 4, Origin: model.OriginClient}
	tests := []struct {
		name            string
		args            args
		want            model.PutResult
		wantErr         error
		wantColdStores  uint64
		wantHotStores   uint64
		wantCreateCalls uint64
		wantAccessCalls uint64
		buildMocks      func(*minimock.Controller, args) upstreamMocks
	}{
		{
			name:            "COLD write registers a COLD version",
			args:            input,
			wantColdStores:  1,
			wantCreateCalls: 1,
			wantAccessCalls: 1,
			want:            model.PutResult{ETag: "etag", VersionID: "version"},
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				m.meta.ChooseWriteClassMock.Expect(a.ctx, a.bucket, a.key, a.size).Return(model.StorageClassCold, nil)
				m.cold.StoreMock.Expect(a.ctx, a.body, a.size, a.contentType).Return(stored, nil)
				expectCreate(m, a, model.StorageClassCold, "version", nil)
				m.access.ProduceObjectWrittenMock.Expect(a.ctx, written).Return()
				return m
			},
		},
		{
			name:            "HOT write goes to the HOT Storage",
			args:            input,
			wantHotStores:   1,
			wantCreateCalls: 1,
			wantAccessCalls: 1,
			want:            model.PutResult{ETag: "etag", VersionID: "version"},
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				m.meta.ChooseWriteClassMock.Expect(a.ctx, a.bucket, a.key, a.size).Return(model.StorageClassHot, nil)
				m.hot.StoreMock.Expect(a.ctx, a.body, a.size, a.contentType).Return(stored, nil)
				expectCreate(m, a, model.StorageClassHot, "version", nil)
				m.access.ProduceObjectWrittenMock.Expect(a.ctx, written).Return()
				return m
			},
		},
		{
			name:    "bucket lookup error",
			args:    input,
			wantErr: upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(false, upstreamErr)
				return m
			},
		},
		{
			name:    "missing bucket",
			args:    input,
			wantErr: domainerrors.ErrBucketNotFound,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(false, nil)
				return m
			},
		},
		{
			name:    "class choice error stores nothing",
			args:    input,
			wantErr: upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				m.meta.ChooseWriteClassMock.Return("", upstreamErr)
				return m
			},
		},
		{
			name:    "unknown class stores nothing",
			args:    input,
			wantErr: domainerrors.ErrUnknownStorageClass,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				m.meta.ChooseWriteClassMock.Return("WARM", nil)
				return m
			},
		},
		{
			name:           "storage error",
			args:           input,
			wantColdStores: 1,
			wantErr:        upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				m.meta.ChooseWriteClassMock.Return(model.StorageClassCold, nil)
				m.cold.StoreMock.Expect(a.ctx, a.body, a.size, a.contentType).Return(model.StoredObject{}, upstreamErr)
				return m
			},
		},
		{
			name:            "metadata registration error",
			args:            input,
			wantColdStores:  1,
			wantCreateCalls: 1,
			wantErr:         upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.HeadBucketMock.Expect(a.ctx, a.bucket).Return(true, nil)
				m.meta.ChooseWriteClassMock.Return(model.StorageClassCold, nil)
				m.cold.StoreMock.Expect(a.ctx, a.body, a.size, a.contentType).Return(stored, nil)
				expectCreate(m, a, model.StorageClassCold, "", upstreamErr)
				return m
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			m := tt.buildMocks(mc, tt.args)
			result, err := objectservice.NewService(m.meta, m.storage(), m.access).Put(tt.args.ctx, tt.args.bucket, tt.args.key, tt.args.body, tt.args.size, tt.args.contentType)
			require.Equal(t, tt.want, result)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantColdStores, m.cold.StoreAfterCounter())
			require.Equal(t, tt.wantHotStores, m.hot.StoreAfterCounter())
			require.Equal(t, tt.wantCreateCalls, m.meta.CreateVersionAfterCounter())
			require.Equal(t, tt.wantAccessCalls, m.access.ProduceObjectWrittenAfterCounter(), "only a registered version is published")
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
	m := newUpstreamMocks(mc)
	m.meta.HeadBucketMock.Expect(ctx, "bucket").Return(true, nil)
	m.meta.ChooseWriteClassMock.Return(model.StorageClassCold, nil)
	m.cold.StoreMock.Set(func(context.Context, io.Reader, *int64, string) (model.StoredObject, error) {
		cancel()
		return stored, nil
	})
	m.meta.CreateVersionMock.Set(func(ctx context.Context, _, _, blobID string, _ int64, _, _ string, _ model.StorageClass) (string, error) {
		require.NoError(t, ctx.Err())
		require.Equal(t, "blob", blobID)
		return "version", nil
	})
	m.access.ProduceObjectWrittenMock.Return()
	result, err := objectservice.NewService(m.meta, m.storage(), m.access).Put(ctx, "bucket", "key", strings.NewReader("data"), &size, "text/plain")
	require.NoError(t, err)
	require.Equal(t, model.PutResult{ETag: "etag", VersionID: "version"}, result)
}
