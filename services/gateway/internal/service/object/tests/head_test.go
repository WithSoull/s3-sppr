package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	objectservice "github.com/alesplll/opens3-rebac/services/gateway/internal/service/object"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestHead(t *testing.T) {
	type args struct {
		ctx         context.Context
		bucket, key string
	}
	input := args{ctx: context.Background(), bucket: "bucket", key: "nested/key"}
	meta := model.ObjectMeta{BlobID: "blob", VersionID: "version", Size: 10, ETag: "etag", ContentType: "text/plain", StorageClass: model.StorageClassCold}
	hotMeta := meta
	hotMeta.StorageClass = model.StorageClassHot
	metadataErr := errors.New("metadata failed")
	tests := []struct {
		name       string
		args       args
		want       model.ObjectMeta
		wantErr    error
		buildMocks func(*minimock.Controller, args) upstreamMocks
	}{
		{
			name: "COLD metadata", args: input, want: meta,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(meta, nil)
				return m
			},
		},
		{
			name: "HOT metadata", args: input, want: hotMeta,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(hotMeta, nil)
				return m
			},
		},
		{
			name: "metadata error is propagated", args: input, wantErr: metadataErr,
			buildMocks: func(mc *minimock.Controller, a args) upstreamMocks {
				m := newUpstreamMocks(mc)
				m.meta.GetObjectMock.Expect(a.ctx, a.bucket, a.key).Return(model.ObjectMeta{}, metadataErr)
				return m
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			m := tt.buildMocks(mc, tt.args)
			result, err := objectservice.NewService(m.meta, m.storage(), m.access).Head(tt.args.ctx, tt.args.bucket, tt.args.key)
			require.Equal(t, tt.want, result)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, 1, m.meta.GetObjectAfterCounter())
			require.Zero(t, m.meta.HeadBucketAfterCounter())
			require.Zero(t, m.meta.ChooseWriteClassAfterCounter())
			require.Zero(t, m.meta.CreateVersionAfterCounter())
			require.Zero(t, m.meta.DeleteObjectAfterCounter())
			require.Zero(t, m.cold.RetrieveAfterCounter())
			require.Zero(t, m.hot.RetrieveAfterCounter())
			require.Zero(t, m.cold.StoreAfterCounter())
			require.Zero(t, m.hot.StoreAfterCounter())
			require.Zero(t, m.access.ProduceObjectReadAfterCounter())
			require.Zero(t, m.access.ProduceObjectWrittenAfterCounter())
		})
	}
}
