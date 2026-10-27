package service

import (
	"context"
	"io"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
)

type ObjectService interface {
	Put(ctx context.Context, bucket, key string, body io.Reader, size *int64, contentType string) (model.PutResult, error)
	// Get reads the whole current version when byteRange is nil.
	Get(ctx context.Context, bucket, key string, byteRange *model.ByteRange) (model.GetResult, error)
	// Head reads current version metadata without Storage access or a demand event.
	Head(ctx context.Context, bucket, key string) (model.ObjectMeta, error)
	// Delete removes the object from the catalog; its blobs stay in Storage.
	Delete(ctx context.Context, bucket, key string) error
}
