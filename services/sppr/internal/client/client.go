package client

import (
	"context"
	"io"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

// PlacementClient calls Metadata PlacementService. Errors are model.Err*
// sentinels mapped from gRPC codes, so callers do not depend on gRPC.
type PlacementClient interface {
	GetPlacement(ctx context.Context, versionID string) (model.Placement, error)
	ReserveCopy(ctx context.Context, a model.ActionSpec) (model.PlacementCopy, error)
	SwitchPlacement(ctx context.Context, req model.SwitchRequest) error
	AbortCopy(ctx context.Context, migrationID, reason string) error
	ListMigrationCopies(ctx context.Context, migrationID string) ([]model.PlacementCopy, error)
	ListDueCleanups(ctx context.Context, limit int32) ([]model.PlacementCopy, error)
	CompleteCleanup(ctx context.Context, copyID string, deleted bool, message string) error
	// ListCatalog pages through current versions and overwritten versions in HOT.
	ListCatalog(ctx context.Context) ([]model.Placement, error)
	ListStorageClasses(ctx context.Context) ([]model.StorageClassUsage, error)
}

// StorageClient talks to the Storage instance of one class.
type StorageClient interface {
	Delete(ctx context.Context, blobID string) error
	// Open starts reading the whole blob; a missing blob fails here with
	// model.ErrUpstreamNotFound.
	Open(ctx context.Context, blobID string) (io.ReadCloser, error)
	// Store writes body under blobID, replacing an earlier copy, and returns the
	// MD5 Storage computed. Storage rejects a body whose length differs from size.
	Store(ctx context.Context, blobID string, size int64, body io.Reader) (string, error)
}
