package model

import "time"

type Placement struct {
	VersionID    string    `db:"version_id"`
	ObjectID     string    `db:"object_id"`
	BucketName   string    `db:"bucket_name"`
	Key          string    `db:"key"`
	IsCurrent    bool      `db:"is_current"`
	StorageClass string    `db:"storage_class"`
	BlobID       string    `db:"blob_id"`
	Generation   int64     `db:"generation"`
	SizeBytes    int64     `db:"size_bytes"`
	UpdatedAt    time.Time `db:"updated_at"`
}

type StorageClassUsage struct {
	Name           string  `db:"name"`
	CapacityBytes  int64   `db:"capacity_bytes"`
	MaxUtilization float64 `db:"max_utilization"`
	UsableBytes    int64   `db:"usable_bytes"`
	PlacedBytes    int64   `db:"placed_bytes"`
	PlacedVersions int64   `db:"placed_versions"`
	CopyBytes      int64   `db:"copy_bytes"`
	Copies         int64   `db:"copies"`
	FailedCleanups int64   `db:"failed_cleanups"`
}
