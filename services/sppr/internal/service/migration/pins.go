package migration

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

func (s *migrationService) ListPins(ctx context.Context) ([]model.Pin, error) {
	return s.pins.List(ctx)
}

func (s *migrationService) AddPin(ctx context.Context, pin model.Pin) (model.Pin, error) {
	if pin.Bucket == "" || pin.Key == "" {
		return model.Pin{}, invalid("bucket and key are required")
	}
	return s.pins.Create(ctx, pin)
}

func (s *migrationService) DeletePin(ctx context.Context, bucket, key string) error {
	if bucket == "" || key == "" {
		return invalid("bucket and key are required")
	}
	return s.pins.Delete(ctx, bucket, key)
}
