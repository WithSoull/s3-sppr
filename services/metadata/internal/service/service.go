package service

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
)

type BucketService interface {
	CreateBucket(ctx context.Context, name, ownerID string) (*model.Bucket, error)
	DeleteBucket(ctx context.Context, name string) error
	GetBucket(ctx context.Context, name string) (*model.Bucket, error)
	ListBuckets(ctx context.Context, ownerID string) ([]*model.Bucket, error)
	HeadBucket(ctx context.Context, name string) (exists bool, bucketID string, ownerID string, err error)
}

type ObjectService interface {
	CreateObjectVersion(ctx context.Context, bucketName, key, blobID string, sizeBytes int64, etag, contentType string) (objectID, versionID string, createdAt time.Time, err error)
	GetObjectMeta(ctx context.Context, bucketName, key, versionID string) (*model.ObjectMeta, error)
	DeleteObjectMeta(ctx context.Context, bucketName, key string) (objectID, blobID string, err error)
	ListObjects(ctx context.Context, bucketName, prefix, continuationToken string, maxKeys int32) ([]*model.ObjectListItem, string, bool, error)
	HealthCheck(ctx context.Context) (pgOK bool, kafkaOK bool)
}

// PlacementService is the API of the migration executor: it changes where a
// version is stored without exposing a way to skip copy verification or CAS.
type PlacementService interface {
	GetPlacement(ctx context.Context, versionID string) (*model.Placement, error)
	ReserveCopy(ctx context.Context, migrationID, versionID string, expectedGeneration int64, sourceClass, targetClass model.StorageClass) (*model.PlacementCopy, error)
	SwitchPlacement(ctx context.Context, migrationID string, verifiedSizeBytes int64, verifiedMD5 string, sourceCleanupDelay time.Duration) (*model.SwitchResult, error)
	AbortCopy(ctx context.Context, migrationID, reason string) (*model.PlacementCopy, error)
	ListMigrationCopies(ctx context.Context, migrationID string) ([]*model.PlacementCopy, error)
	ListDueCleanups(ctx context.Context, limit int32) ([]*model.PlacementCopy, error)
	CompleteCleanup(ctx context.Context, copyID string, deleted bool, errorMessage string) (*model.PlacementCopy, error)
}
