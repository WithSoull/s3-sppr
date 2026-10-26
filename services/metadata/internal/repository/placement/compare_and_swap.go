package placement

import (
	"context"
	"errors"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/jackc/pgx/v4"
)

// CompareAndSwap moves the placement to a new blob only if generation and class
// still match what the migration verified, and returns the new generation.
func (r *repo) CompareAndSwap(
	ctx context.Context,
	versionID string,
	expectedGeneration int64,
	expectedClass, newClass model.StorageClass,
	newBlobID string,
) (int64, error) {
	q := db.Query{
		Name: "placement_repository:CompareAndSwap",
		QueryRaw: `
UPDATE version_placements
SET storage_class = $4,
    blob_id = $5,
    generation = generation + 1,
    updated_at = now()
WHERE version_id = $1 AND generation = $2 AND storage_class = $3
RETURNING generation`,
	}

	var generation int64
	err := r.db.DB().QueryRowContext(ctx, q, versionID, expectedGeneration, expectedClass, newClass, newBlobID).Scan(&generation)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, domainerrors.ErrPlacementChanged
		}
		return 0, err
	}

	return generation, nil
}
