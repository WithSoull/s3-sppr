package placement

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

// Create records the initial placement of a committed version.
// Used inside CreateObjectVersion transaction after InsertVersion.
func (r *repo) Create(ctx context.Context, versionID, blobID string, storageClass model.StorageClass) error {
	q := db.Query{
		Name: "placement_repository:Create",
		QueryRaw: `
INSERT INTO version_placements (version_id, storage_class, blob_id)
VALUES ($1, $2, $3)`,
	}

	_, err := r.db.DB().ExecContext(ctx, q, versionID, storageClass, blobID)
	return err
}
