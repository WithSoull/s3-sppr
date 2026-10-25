package object

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
)

func (s *objectService) Get(ctx context.Context, bucket, key string) (model.GetResult, error) {
	meta, err := s.metadata.GetObject(ctx, bucket, key)
	if err != nil {
		return model.GetResult{}, err
	}
	result := model.GetResult{
		Size: meta.Size, ETag: meta.ETag, VersionID: meta.VersionID, ContentType: meta.ContentType,
	}
	body, err := s.storage.Retrieve(ctx, meta.BlobID, meta.Size)
	if err != nil {
		return result, err
	}
	result.Body = body
	return result, nil
}
