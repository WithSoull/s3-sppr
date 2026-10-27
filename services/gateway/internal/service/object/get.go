package object

import (
	"context"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
)

func (s *objectService) Get(ctx context.Context, bucket, key string, byteRange *model.ByteRange) (model.GetResult, error) {
	meta, err := s.metadata.GetObject(ctx, bucket, key)
	if err != nil {
		return model.GetResult{}, err
	}
	length := meta.Size
	var contentRange *model.ContentRange
	if byteRange != nil {
		offset, rangeLength, ok := byteRange.Resolve(meta.Size)
		if !ok {
			return model.GetResult{}, &domainerrors.RangeNotSatisfiableError{Size: meta.Size}
		}
		length = rangeLength
		contentRange = &model.ContentRange{Offset: offset, Length: length}
	}
	storage, err := s.storageFor(meta.StorageClass)
	if err != nil {
		return model.GetResult{}, err
	}
	body, err := storage.Retrieve(ctx, meta.BlobID, meta.Size, contentRange)
	if err != nil {
		return model.GetResult{}, err
	}
	// The stream is open: a response interrupted later still counts as demand.
	s.access.ProduceObjectRead(ctx, model.ObjectReadEvent{
		Bucket:    bucket,
		ObjectKey: key,
		VersionID: meta.VersionID,
		Bytes:     length,
		Origin:    model.OriginClient,
	})
	return model.GetResult{
		Body:        body,
		Size:        meta.Size,
		Range:       contentRange,
		ETag:        meta.ETag,
		VersionID:   meta.VersionID,
		ContentType: meta.ContentType,
	}, nil
}
