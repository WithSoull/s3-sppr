package placementcopy

import (
	"context"
	"errors"
	"time"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/jackc/pgconn"
)

const uniqueViolation = "23505"

// InsertTarget reserves space for a new blob of the version in targetClass and
// records the placement generation and class the switch must still find.
func (r *repo) InsertTarget(ctx context.Context, migrationID string, placement *model.Placement, targetClass model.StorageClass) (*model.PlacementCopy, error) {
	q := db.Query{
		Name: "copy_repository:InsertTarget",
		QueryRaw: `
INSERT INTO placement_copies (
    migration_id, version_id, role, storage_class, size_bytes,
    expected_generation, expected_class, state
)
VALUES ($1, $2, 'target', $3, $4, $5, $6, 'reserved')
RETURNING` + copyColumns,
	}

	c, err := r.scanOne(ctx, q,
		migrationID,
		placement.VersionID,
		targetClass,
		placement.SizeBytes,
		placement.Generation,
		placement.StorageClass,
	)
	if err != nil {
		// Either another migration holds a reserved copy of this version, or a
		// concurrent retry of the same migration inserted first.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return nil, domainerrors.ErrCopyAlreadyReserved
		}
		return nil, err
	}

	return c, nil
}

// InsertSource keeps the blob replaced by a switch until reads started before the
// switch can finish.
func (r *repo) InsertSource(ctx context.Context, migrationID string, placement *model.Placement, cleanupDelay time.Duration) (*model.PlacementCopy, error) {
	q := db.Query{
		Name: "copy_repository:InsertSource",
		QueryRaw: `
INSERT INTO placement_copies (
    migration_id, version_id, role, storage_class, blob_id, size_bytes,
    state, cleanup_after
)
VALUES ($1, $2, 'source', $3, $4, $5, 'cleanup_pending', now() + $6::bigint * interval '1 millisecond')
RETURNING` + copyColumns,
	}

	return r.scanOne(ctx, q,
		migrationID,
		placement.VersionID,
		placement.StorageClass,
		placement.BlobID,
		placement.SizeBytes,
		cleanupDelay.Milliseconds(),
	)
}
