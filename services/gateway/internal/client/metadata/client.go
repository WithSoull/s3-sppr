package metadata

import (
	"context"
	"time"

	contracts "github.com/alesplll/opens3-rebac/services/gateway/internal/client"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

type client struct {
	grpc    metadatav1.MetadataServiceClient
	timeout time.Duration
}

func NewClient(grpcClient metadatav1.MetadataServiceClient, timeout time.Duration) contracts.MetadataClient {
	return &client{grpc: grpcClient, timeout: timeout}
}

func (c *client) HeadBucket(ctx context.Context, bucket string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.grpc.HeadBucket(ctx, &metadatav1.HeadBucketRequest{BucketName: bucket})
	if err != nil {
		return false, mapError(err)
	}
	return response.GetExists(), nil
}

func (c *client) ChooseWriteClass(ctx context.Context, bucket, key string, size *int64) (model.StorageClass, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.grpc.ChooseWriteClass(ctx, &metadatav1.ChooseWriteClassRequest{BucketName: bucket, Key: key, SizeBytes: size})
	if err != nil {
		return "", mapError(err)
	}
	return model.StorageClass(response.GetStorageClass()), nil
}

func (c *client) CreateVersion(ctx context.Context, bucket, key, blobID string, size int64, etag, contentType string, class model.StorageClass) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.grpc.CreateObjectVersion(ctx, &metadatav1.CreateObjectVersionRequest{
		BucketName:   bucket,
		Key:          key,
		BlobId:       blobID,
		SizeBytes:    size,
		Etag:         etag,
		ContentType:  contentType,
		StorageClass: string(class),
	})
	if err != nil {
		return "", mapError(err)
	}
	return response.GetVersionId(), nil
}

func (c *client) GetObject(ctx context.Context, bucket, key string) (model.ObjectMeta, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.grpc.GetObjectMeta(ctx, &metadatav1.GetObjectMetaRequest{BucketName: bucket, Key: key})
	if err != nil {
		return model.ObjectMeta{}, mapError(err)
	}
	return model.ObjectMeta{
		BlobID:       response.GetBlobId(),
		VersionID:    response.GetVersionId(),
		Size:         response.GetSizeBytes(),
		ETag:         response.GetEtag(),
		ContentType:  response.GetContentType(),
		StorageClass: model.StorageClass(response.GetStorageClass()),
	}, nil
}

func (c *client) DeleteObject(ctx context.Context, bucket, key string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.grpc.DeleteObjectMeta(ctx, &metadatav1.DeleteObjectMetaRequest{BucketName: bucket, Key: key})
	return mapError(err)
}
