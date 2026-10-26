package placementcopy

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

func (r *repo) Promote(ctx context.Context, copyID, verifiedMD5 string) error {
	q := db.Query{
		Name: "copy_repository:Promote",
		QueryRaw: `
UPDATE placement_copies
SET state = 'promoted', verified_md5 = $2, updated_at = now()
WHERE copy_id = $1`,
	}

	_, err := r.db.DB().ExecContext(ctx, q, copyID, verifiedMD5)
	return err
}

func (r *repo) ScheduleCleanup(ctx context.Context, copyID string, delay time.Duration, reason string) (*model.PlacementCopy, error) {
	return r.scanOne(ctx, db.Query{
		Name: "copy_repository:ScheduleCleanup",
		QueryRaw: `
UPDATE placement_copies
SET state = 'cleanup_pending',
    cleanup_after = now() + $2::bigint * interval '1 millisecond',
    last_error = $3,
    updated_at = now()
WHERE copy_id = $1
RETURNING` + copyColumns,
	}, copyID, delay.Milliseconds(), reason)
}

func (r *repo) MarkDeleted(ctx context.Context, copyID string) (*model.PlacementCopy, error) {
	return r.scanOne(ctx, db.Query{
		Name: "copy_repository:MarkDeleted",
		QueryRaw: `
UPDATE placement_copies
SET state = 'deleted', cleanup_attempts = cleanup_attempts + 1, updated_at = now()
WHERE copy_id = $1
RETURNING` + copyColumns,
	}, copyID)
}

func (r *repo) MarkCleanupFailed(ctx context.Context, copyID, reason string) (*model.PlacementCopy, error) {
	return r.scanOne(ctx, db.Query{
		Name: "copy_repository:MarkCleanupFailed",
		QueryRaw: `
UPDATE placement_copies
SET state = 'cleanup_failed', cleanup_attempts = cleanup_attempts + 1, last_error = $2, updated_at = now()
WHERE copy_id = $1
RETURNING` + copyColumns,
	}, copyID, reason)
}
