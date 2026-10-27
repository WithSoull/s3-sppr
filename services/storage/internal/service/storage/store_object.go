package storage

import (
	"context"
	"io"

	domainerrors "github.com/alesplll/opens3-rebac/services/storage/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/storage/internal/model"
	"github.com/alesplll/opens3-rebac/services/storage/internal/observability"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const defaultContentType = "application/octet-stream"

func (s *storageService) StoreObject(ctx context.Context, blobID string, reader io.Reader, size *int64, contentType string) (*model.BlobMeta, error) {
	if size != nil && *size < 0 {
		return nil, domainerrors.ErrInvalidBlobSize
	}
	// The ID becomes a file path, so only the canonical UUID form is accepted.
	if blobID != "" {
		if parsed, err := uuid.Parse(blobID); err != nil || parsed.String() != blobID {
			return nil, domainerrors.ErrInvalidBlobID
		}
	} else {
		blobID = uuid.New().String()
	}

	if contentType == "" {
		contentType = defaultContentType
	}

	meta, err := s.repo.StoreBlob(ctx, blobID, reader, size)
	if err != nil {
		logger.Error(
			ctx,
			"failed to store blob",
			zap.Error(err),
			zap.String("blob_id", blobID),
			zap.String("content_type", contentType),
		)
		return nil, err
	}

	meta.ContentType = contentType
	observability.AddWriteBytes(ctx, meta.SizeBytes)
	logger.Info(
		ctx,
		"stored blob",
		zap.String("blob_id", meta.BlobID),
		zap.Int64("size_bytes", meta.SizeBytes),
		zap.String("content_type", meta.ContentType),
	)
	return meta, nil
}
