package tests

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/client"
	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	objectservice "github.com/alesplll/opens3-rebac/services/gateway/internal/service/object"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

type upstreamMocks struct {
	meta   *mocks.MetadataClientMock
	hot    *mocks.StorageClientMock
	cold   *mocks.StorageClientMock
	access *mocks.AccessProducerServiceMock
}

func newUpstreamMocks(mc *minimock.Controller) upstreamMocks {
	return upstreamMocks{
		meta:   mocks.NewMetadataClientMock(mc),
		hot:    mocks.NewStorageClientMock(mc),
		cold:   mocks.NewStorageClientMock(mc),
		access: mocks.NewAccessProducerServiceMock(mc),
	}
}

func (m upstreamMocks) storage() map[model.StorageClass]client.StorageClient {
	return map[model.StorageClass]client.StorageClient{model.StorageClassHot: m.hot, model.StorageClassCold: m.cold}
}

func TestGet(t *testing.T) {
	type args struct {
		ctx         context.Context
		bucket, key string
		byteRange   *model.ByteRange
	}
	upstreamErr := errors.New("upstream failed")
	input := args{ctx: context.Background(), bucket: "bucket", key: "nested/key"}
	coldMeta := model.ObjectMeta{BlobID: "blob", VersionID: "version", Size: 10, ETag: "etag", ContentType: "text/plain", StorageClass: model.StorageClassCold}
	hotMeta := coldMeta
	hotMeta.StorageClass = model.StorageClassHot
	body := io.NopCloser(strings.NewReader("0123456789"))
	readEvent := func(bytes int64) model.ObjectReadEvent {
		return model.ObjectReadEvent{Bucket: "bucket", ObjectKey: "nested/key", VersionID: "version", Bytes: bytes, Origin: model.OriginClient}
	}
	tests := []struct {
		name            string
		args            args
		want            model.GetResult
		wantErr         error
		wantColdCalls   uint64
		wantHotCalls    uint64
		wantAccessCalls uint64
		buildMocks      func(*minimock.Controller, args) upstreamMocks
	}{
		{
			name:            "whole COLD object",
			args:            input,
			wantColdCalls:   1,
			wantAccessCalls: 1,
			want:            model.GetResult{Body: body, Size: 10, ETag: "etag", VersionID: "version", ContentType: "text/plain"},
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(coldMeta, nil)
				m.cold.RetrieveMock.Expect(a.ctx, "blob", 10, nil).Return(body, nil)
				m.access.ProduceObjectReadMock.Expect(a.ctx, readEvent(10)).Return()
				return m
			},
		},
		{
			name:            "HOT object is read from the HOT Storage",
			args:            input,
			wantHotCalls:    1,
			wantAccessCalls: 1,
			want:            model.GetResult{Body: body, Size: 10, ETag: "etag", VersionID: "version", ContentType: "text/plain"},
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(hotMeta, nil)
				m.hot.RetrieveMock.Expect(a.ctx, "blob", 10, nil).Return(body, nil)
				m.access.ProduceObjectReadMock.Expect(a.ctx, readEvent(10)).Return()
				return m
			},
		},
		{
			name:            "range reads only its bytes and reports them as demand",
			args:            args{ctx: input.ctx, bucket: input.bucket, key: input.key, byteRange: &model.ByteRange{First: 2, Last: 4}},
			wantColdCalls:   1,
			wantAccessCalls: 1,
			want: model.GetResult{Body: body, Size: 10, Range: &model.ContentRange{Offset: 2, Length: 3},
				ETag: "etag", VersionID: "version", ContentType: "text/plain"},
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(coldMeta, nil)
				m.cold.RetrieveMock.Expect(a.ctx, "blob", 10, &model.ContentRange{Offset: 2, Length: 3}).Return(body, nil)
				m.access.ProduceObjectReadMock.Expect(a.ctx, readEvent(3)).Return()
				return m
			},
		},
		{
			name:            "If-Range with the current ETag keeps the range",
			args:            args{ctx: input.ctx, bucket: input.bucket, key: input.key, byteRange: &model.ByteRange{First: 2, Last: 4, IfRangeETag: "etag"}},
			wantColdCalls:   1,
			wantAccessCalls: 1,
			want: model.GetResult{Body: body, Size: 10, Range: &model.ContentRange{Offset: 2, Length: 3},
				ETag: "etag", VersionID: "version", ContentType: "text/plain"},
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(coldMeta, nil)
				m.cold.RetrieveMock.Expect(a.ctx, "blob", 10, &model.ContentRange{Offset: 2, Length: 3}).Return(body, nil)
				m.access.ProduceObjectReadMock.Expect(a.ctx, readEvent(3)).Return()
				return m
			},
		},
		{
			name:            "If-Range with another ETag reads the whole object",
			args:            args{ctx: input.ctx, bucket: input.bucket, key: input.key, byteRange: &model.ByteRange{First: 2, Last: 4, IfRangeETag: "old-etag"}},
			wantColdCalls:   1,
			wantAccessCalls: 1,
			want:            model.GetResult{Body: body, Size: 10, ETag: "etag", VersionID: "version", ContentType: "text/plain"},
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(coldMeta, nil)
				m.cold.RetrieveMock.Expect(a.ctx, "blob", 10, nil).Return(body, nil)
				m.access.ProduceObjectReadMock.Expect(a.ctx, readEvent(10)).Return()
				return m
			},
		},
		{
			name:    "range past the end is not satisfiable",
			args:    args{ctx: input.ctx, bucket: input.bucket, key: input.key, byteRange: &model.ByteRange{First: 10, Last: -1}},
			wantErr: domainerrors.ErrRangeNotSatisfiable,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(coldMeta, nil)
				return m
			},
		},
		{
			name:    "unknown storage class",
			args:    input,
			wantErr: domainerrors.ErrUnknownStorageClass,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				warm := coldMeta
				warm.StorageClass = "WARM"
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(warm, nil)
				return m
			},
		},
		{
			name:    "metadata error",
			args:    input,
			wantErr: upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(model.ObjectMeta{}, upstreamErr)
				return m
			},
		},
		{
			name:          "storage error",
			args:          input,
			wantColdCalls: 1,
			wantErr:       upstreamErr,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(coldMeta, nil)
				m.cold.RetrieveMock.Expect(a.ctx, "blob", 10, nil).Return(nil, upstreamErr)
				return m
			},
		},
		{
			name:          "empty storage stream",
			args:          input,
			wantColdCalls: 1,
			wantErr:       domainerrors.ErrEmptyStorageStream,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(coldMeta, nil)
				m.cold.RetrieveMock.Expect(a.ctx, "blob", 10, nil).Return(nil, domainerrors.ErrEmptyStorageStream)
				return m
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			m := tt.buildMocks(mc, tt.args)
			result, err := objectservice.NewService(m.meta, m.storage(), m.access).Get(tt.args.ctx, tt.args.bucket, tt.args.key, tt.args.byteRange)
			require.Equal(t, tt.want, result)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			var rangeErr *domainerrors.RangeNotSatisfiableError
			if errors.As(err, &rangeErr) {
				require.Equal(t, coldMeta.Size, rangeErr.Size, "416 must report the object size")
			}
			require.Equal(t, tt.wantColdCalls, m.cold.RetrieveAfterCounter())
			require.Equal(t, tt.wantHotCalls, m.hot.RetrieveAfterCounter())
			require.Equal(t, tt.wantAccessCalls, m.access.ProduceObjectReadAfterCounter(), "only a successful Get is published")
			if result.Body != nil {
				require.NoError(t, result.Body.Close())
			}
		})
	}
}
