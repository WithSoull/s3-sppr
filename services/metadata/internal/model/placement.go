package model

import "time"

func (c StorageClass) Valid() bool {
	return c == StorageClassHot || c == StorageClassCold
}

// Placement is the single location reads use for a committed version.
type Placement struct {
	VersionID    string
	ObjectID     string
	BucketName   string
	Key          string
	IsCurrent    bool
	StorageClass StorageClass
	BlobID       string
	Generation   int64
	SizeBytes    int64
	UpdatedAt    time.Time
	// MigrationInProgress is set while a target copy of the version is reserved.
	MigrationInProgress bool
}

type CopyRole string

const (
	CopyRoleTarget CopyRole = "target"
	CopyRoleSource CopyRole = "source"
)

type CopyState string

const (
	CopyStateReserved       CopyState = "reserved"
	CopyStatePromoted       CopyState = "promoted"
	CopyStateCleanupPending CopyState = "cleanup_pending"
	CopyStateCleanupFailed  CopyState = "cleanup_failed"
	CopyStateDeleted        CopyState = "deleted"
)

// PlacementCopy is a blob of a version outside its placement: a migration
// target before CAS or a blob waiting for deletion.
type PlacementCopy struct {
	CopyID             string
	MigrationID        string
	VersionID          *string
	Role               CopyRole
	StorageClass       StorageClass
	BlobID             string
	SizeBytes          int64
	ExpectedGeneration *int64
	ExpectedClass      *StorageClass
	State              CopyState
	VerifiedMD5        string
	CleanupAfter       *time.Time
	CleanupAttempts    int32
	LastError          string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// StorageClassUsage counts placements of all versions, including non-current
// ones, and copies that still occupy space.
type StorageClassUsage struct {
	Name           StorageClass
	CapacityBytes  int64
	MaxUtilization float64
	UsableBytes    int64
	PlacedBytes    int64
	PlacedVersions int64
	CopyBytes      int64
	Copies         int64
	FailedCleanups int64
}

func (u StorageClassUsage) FreeBytes() int64 {
	return u.UsableBytes - u.PlacedBytes - u.CopyBytes
}

// SwitchResult describes a completed CAS of a version placement.
type SwitchResult struct {
	VersionID     string
	NewGeneration int64
	StorageClass  StorageClass
	BlobID        string
	SourceCopy    *PlacementCopy
}
