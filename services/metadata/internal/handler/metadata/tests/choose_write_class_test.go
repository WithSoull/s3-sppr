package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestChooseWriteClass(t *testing.T) {
	t.Run("passes known size", func(t *testing.T) {
		ctx := context.Background()
		mc := minimock.NewController(t)

		objectServiceMock := mocks.NewObjectServiceMock(mc)
		objectServiceMock.ChooseWriteClassMock.Inspect(func(_ context.Context, bucketName, key string, sizeBytes *int64) {
			require.Equal(t, "photos", bucketName)
			require.Equal(t, "cats/1.jpg", key)
			require.NotNil(t, sizeBytes)
			require.Equal(t, int64(123), *sizeBytes)
		}).Return(&model.WriteClassDecision{StorageClass: model.StorageClassHot, Reason: model.WriteClassReasonInherited}, nil)

		res, err := newHandler(nil, objectServiceMock).ChooseWriteClass(ctx, &metadatav1.ChooseWriteClassRequest{
			BucketName: "photos",
			Key:        "cats/1.jpg",
			SizeBytes:  proto.Int64(123),
		})

		require.NoError(t, err)
		require.Equal(t, &metadatav1.ChooseWriteClassResponse{StorageClass: "HOT", Reason: "inherited"}, res)
	})

	t.Run("passes unknown size as nil", func(t *testing.T) {
		ctx := context.Background()
		mc := minimock.NewController(t)

		objectServiceMock := mocks.NewObjectServiceMock(mc)
		objectServiceMock.ChooseWriteClassMock.Expect(ctx, "photos", "cats/1.jpg", nil).
			Return(&model.WriteClassDecision{StorageClass: model.StorageClassCold, Reason: model.WriteClassReasonSizeUnknown}, nil)

		res, err := newHandler(nil, objectServiceMock).ChooseWriteClass(ctx, &metadatav1.ChooseWriteClassRequest{
			BucketName: "photos",
			Key:        "cats/1.jpg",
		})

		require.NoError(t, err)
		require.Equal(t, &metadatav1.ChooseWriteClassResponse{StorageClass: "COLD", Reason: "size_unknown"}, res)
	})

	t.Run("service error", func(t *testing.T) {
		ctx := context.Background()
		serviceErr := errors.New("service error")
		mc := minimock.NewController(t)

		objectServiceMock := mocks.NewObjectServiceMock(mc)
		objectServiceMock.ChooseWriteClassMock.Return(nil, serviceErr)

		res, err := newHandler(nil, objectServiceMock).ChooseWriteClass(ctx, &metadatav1.ChooseWriteClassRequest{
			BucketName: "photos",
			Key:        "cats/1.jpg",
		})

		require.Nil(t, res)
		require.ErrorIs(t, err, serviceErr)
	})
}
