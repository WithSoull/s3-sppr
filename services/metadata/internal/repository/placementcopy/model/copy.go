package model

import "time"

type PlacementCopy struct {
	CopyID             string     `db:"copy_id"`
	MigrationID        string     `db:"migration_id"`
	VersionID          *string    `db:"version_id"`
	Role               string     `db:"role"`
	StorageClass       string     `db:"storage_class"`
	BlobID             string     `db:"blob_id"`
	SizeBytes          int64      `db:"size_bytes"`
	ExpectedGeneration *int64     `db:"expected_generation"`
	ExpectedClass      *string    `db:"expected_class"`
	State              string     `db:"state"`
	VerifiedMD5        string     `db:"verified_md5"`
	CleanupAfter       *time.Time `db:"cleanup_after"`
	CleanupAttempts    int32      `db:"cleanup_attempts"`
	LastError          string     `db:"last_error"`
	CreatedAt          time.Time  `db:"created_at"`
	UpdatedAt          time.Time  `db:"updated_at"`
}
