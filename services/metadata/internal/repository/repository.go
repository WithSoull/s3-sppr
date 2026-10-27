package repository

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
)

type BucketRepository interface {
	Create(ctx context.Context, name, ownerID string) (*model.Bucket, error)
	Get(ctx context.Context, name string) (*model.Bucket, error)
	List(ctx context.Context, ownerID string) ([]*model.Bucket, error)
	Head(ctx context.Context, name string) (bool, string, string, error)
	Delete(ctx context.Context, bucketID string) error
	CountObjects(ctx context.Context, bucketID string) (int64, error)
}

type ObjectRepository interface {
	UpsertObject(ctx context.Context, bucketName, key string) (string, error)
	InsertVersion(ctx context.Context, objectID, blobID string, sizeBytes int64, etag, contentType string) (string, time.Time, error)
	SetCurrentVersion(ctx context.Context, objectID, versionID string) error
	GetMeta(ctx context.Context, bucketName, key, versionID string) (*model.ObjectMeta, error)
	// LockForDelete locks the object row so concurrent overwrites and placement
	// switches wait until the delete transaction ends.
	LockForDelete(ctx context.Context, bucketName, key string) (string, error)
	Delete(ctx context.Context, bucketName, key string) (string, string, error)
	List(ctx context.Context, bucketName, prefix, continuationToken string, maxKeys int32) ([]*model.ObjectListItem, string, bool, error)
}

type PlacementRepository interface {
	Create(ctx context.Context, versionID, blobID string, storageClass model.StorageClass) error
	Get(ctx context.Context, versionID string) (*model.Placement, error)
	List(ctx context.Context, req model.ListPlacementsRequest) ([]*model.Placement, error)
	// LockForChange locks the object and placement rows of the version until the
	// transaction ends, so overwrites and other switches wait for the caller.
	LockForChange(ctx context.Context, versionID string) (*model.Placement, error)
	CompareAndSwap(ctx context.Context, versionID string, expectedGeneration int64, expectedClass, newClass model.StorageClass, newBlobID string) (int64, error)
	// LockStorageClasses locks source and target in name order after the object
	// and placement, serializing capacity checks without inverting FK locks.
	LockStorageClasses(ctx context.Context, sourceClass, targetClass model.StorageClass) error
	GetUsage(ctx context.Context, storageClass model.StorageClass) (*model.StorageClassUsage, error)
	ListUsage(ctx context.Context) ([]*model.StorageClassUsage, error)
}

type CopyRepository interface {
	InsertTarget(ctx context.Context, migrationID string, placement *model.Placement, targetClass model.StorageClass) (*model.PlacementCopy, error)
	InsertSource(ctx context.Context, migrationID string, placement *model.Placement, cleanupDelay time.Duration) (*model.PlacementCopy, error)
	InsertOrphans(ctx context.Context, objectID string, cleanupDelay time.Duration) error
	GetByMigration(ctx context.Context, migrationID string, role model.CopyRole) (*model.PlacementCopy, error)
	LockByMigration(ctx context.Context, migrationID string, role model.CopyRole) (*model.PlacementCopy, error)
	LockByID(ctx context.Context, copyID string) (*model.PlacementCopy, error)
	ListByMigration(ctx context.Context, migrationID string) ([]*model.PlacementCopy, error)
	ListDueCleanups(ctx context.Context, limit int32) ([]*model.PlacementCopy, error)
	Promote(ctx context.Context, copyID, verifiedMD5 string) error
	ScheduleCleanup(ctx context.Context, copyID string, delay time.Duration, reason string) (*model.PlacementCopy, error)
	MarkDeleted(ctx context.Context, copyID string) (*model.PlacementCopy, error)
	MarkCleanupFailed(ctx context.Context, copyID, reason string) (*model.PlacementCopy, error)
}
