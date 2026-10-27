package client

import (
	"context"
	"io"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
)

type MetadataClient interface {
	HeadBucket(ctx context.Context, bucket string) (bool, error)
	// ChooseWriteClass is asked before the bytes are written; size is nil when unknown.
	ChooseWriteClass(ctx context.Context, bucket, key string, size *int64) (model.StorageClass, error)
	CreateVersion(ctx context.Context, bucket, key, blobID string, size int64, etag, contentType string, class model.StorageClass) (string, error)
	GetObject(ctx context.Context, bucket, key string) (model.ObjectMeta, error)
	DeleteObject(ctx context.Context, bucket, key string) error
}

// StorageClient talks to the Storage instance of one storage class.
type StorageClient interface {
	Store(ctx context.Context, body io.Reader, size *int64, contentType string) (model.StoredObject, error)
	// Retrieve streams the whole blob of the given size, or only part when it is set.
	// A whole read asks Storage for everything, so a blob longer than size is noticed.
	Retrieve(ctx context.Context, blobID string, size int64, part *model.ContentRange) (io.ReadCloser, error)
}
