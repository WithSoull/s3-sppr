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

// InsertOrphans keeps the blobs of every placed version of an object that is being
// deleted, so the space stays counted until cleanup removes them from Storage.
func (r *repo) InsertOrphans(ctx context.Context, objectID string, cleanupDelay time.Duration) error {
	q := db.Query{
		Name: "copy_repository:InsertOrphans",
		QueryRaw: `
INSERT INTO placement_copies (
    version_id, role, storage_class, blob_id, size_bytes, state, cleanup_after
)
SELECT vp.version_id, 'orphan', vp.storage_class, vp.blob_id, v.size_bytes,
       'cleanup_pending', now() + $2::bigint * interval '1 millisecond'
FROM version_placements vp
JOIN versions v ON v.id = vp.version_id
WHERE v.object_id = $1`,
	}

	_, err := r.db.DB().ExecContext(ctx, q, objectID, cleanupDelay.Milliseconds())
	return err
}
