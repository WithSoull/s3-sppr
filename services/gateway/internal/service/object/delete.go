package object

import "context"

func (s *objectService) Delete(ctx context.Context, bucket, key string) error {
	return s.metadata.DeleteObject(ctx, bucket, key)
}
