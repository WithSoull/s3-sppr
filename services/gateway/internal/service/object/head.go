package object

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
)

func (s *objectService) Head(ctx context.Context, bucket, key string) (model.ObjectMeta, error) {
	return s.metadata.GetObject(ctx, bucket, key)
}
