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
		return false, err
	}
	return response.GetExists(), nil
}

func (c *client) CreateVersion(ctx context.Context, bucket, key, blobID string, size int64, etag, contentType string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.grpc.CreateObjectVersion(ctx, &metadatav1.CreateObjectVersionRequest{
		BucketName:  bucket,
		Key:         key,
		BlobId:      blobID,
		SizeBytes:   size,
		Etag:        etag,
		ContentType: contentType,
	})
	if err != nil {
		return "", err
	}
	return response.GetVersionId(), nil
}

func (c *client) GetObject(ctx context.Context, bucket, key string) (model.ObjectMeta, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.grpc.GetObjectMeta(ctx, &metadatav1.GetObjectMetaRequest{BucketName: bucket, Key: key})
	if err != nil {
		return model.ObjectMeta{}, err
	}
	return model.ObjectMeta{
		BlobID:      response.GetBlobId(),
		VersionID:   response.GetVersionId(),
		Size:        response.GetSizeBytes(),
		ETag:        response.GetEtag(),
		ContentType: response.GetContentType(),
	}, nil
}
