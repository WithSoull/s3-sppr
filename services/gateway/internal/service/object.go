package service

import (
	"context"
	"io"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
)

type ObjectService interface {
	Put(ctx context.Context, bucket, key string, body io.Reader, size *int64, contentType string) (model.PutResult, error)
	Get(ctx context.Context, bucket, key string) (model.GetResult, error)
}
