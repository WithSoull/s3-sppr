package object

import (
	"context"
	"io"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

func (s *objectService) Put(ctx context.Context, bucket, key string, body io.Reader, size *int64, contentType string) (model.PutResult, error) {
	exists, err := s.metadata.HeadBucket(ctx, bucket)
	if err != nil {
		return model.PutResult{}, err
	}
	if !exists {
		return model.PutResult{}, domainerrors.ErrBucketNotFound
	}

	stored, err := s.storage.Store(ctx, body, size, contentType)
	if err != nil {
		return model.PutResult{}, err
	}
	// The blob is already committed; a client disconnect must not leave it without a catalog entry.
	versionID, err := s.metadata.CreateVersion(context.WithoutCancel(ctx), bucket, key, stored.BlobID, stored.Size, stored.ETag, contentType)
	if err != nil {
		logger.Error(ctx, "metadata registration failed after blob commit",
			zap.String("blob_id", stored.BlobID), zap.String("bucket", bucket), zap.String("key", key), zap.Error(err))
		return model.PutResult{}, err
	}
	return model.PutResult{ETag: stored.ETag, VersionID: versionID}, nil
}
