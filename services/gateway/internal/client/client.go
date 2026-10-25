package client

import (
	"context"
	"io"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
)

type MetadataClient interface {
	HeadBucket(ctx context.Context, bucket string) (bool, error)
	CreateVersion(ctx context.Context, bucket, key, blobID string, size int64, etag, contentType string) (string, error)
	GetObject(ctx context.Context, bucket, key string) (model.ObjectMeta, error)
}

type StorageClient interface {
	Store(ctx context.Context, body io.Reader, size *int64, contentType string) (model.StoredObject, error)
	Retrieve(ctx context.Context, blobID string, expectedSize int64) (io.ReadCloser, error)
}
