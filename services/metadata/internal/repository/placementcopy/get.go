package placementcopy

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

const maxDueCleanups = 1000

func (r *repo) GetByMigration(ctx context.Context, migrationID string, role model.CopyRole) (*model.PlacementCopy, error) {
	return r.scanOne(ctx, db.Query{
		Name:     "copy_repository:GetByMigration",
		QueryRaw: "SELECT" + copyColumns + "\nFROM placement_copies WHERE migration_id = $1 AND role = $2",
	}, migrationID, role)
}

func (r *repo) LockByMigration(ctx context.Context, migrationID string, role model.CopyRole) (*model.PlacementCopy, error) {
	return r.scanOne(ctx, db.Query{
		Name:     "copy_repository:LockByMigration",
		QueryRaw: "SELECT" + copyColumns + "\nFROM placement_copies WHERE migration_id = $1 AND role = $2 FOR UPDATE",
	}, migrationID, role)
}

func (r *repo) LockByID(ctx context.Context, copyID string) (*model.PlacementCopy, error) {
	return r.scanOne(ctx, db.Query{
		Name:     "copy_repository:LockByID",
		QueryRaw: "SELECT" + copyColumns + "\nFROM placement_copies WHERE copy_id = $1 FOR UPDATE",
	}, copyID)
}

func (r *repo) ListByMigration(ctx context.Context, migrationID string) ([]*model.PlacementCopy, error) {
	return r.scanAll(ctx, db.Query{
		Name:     "copy_repository:ListByMigration",
		QueryRaw: "SELECT" + copyColumns + "\nFROM placement_copies WHERE migration_id = $1 ORDER BY created_at",
	}, migrationID)
}

func (r *repo) ListDueCleanups(ctx context.Context, limit int32) ([]*model.PlacementCopy, error) {
	if limit <= 0 || limit > maxDueCleanups {
		limit = maxDueCleanups
	}

	return r.scanAll(ctx, db.Query{
		Name: "copy_repository:ListDueCleanups",
		QueryRaw: "SELECT" + copyColumns + `
FROM placement_copies
WHERE state IN ('cleanup_pending', 'cleanup_failed') AND cleanup_after <= now()
ORDER BY cleanup_after, copy_id
LIMIT $1`,
	}, limit)
}
