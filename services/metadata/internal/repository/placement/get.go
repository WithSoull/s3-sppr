package placement

import (
	"context"
	"errors"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placement/converter"
	repoModel "github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placement/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/jackc/pgx/v4"
)

const selectPlacements = `
SELECT vp.version_id,
       o.id AS object_id,
       b.name AS bucket_name,
       o.key,
       COALESCE(o.current_version_id = v.id AND o.status = 'active', false) AS is_current,
       vp.storage_class,
       vp.blob_id,
       vp.generation,
       v.size_bytes,
       vp.updated_at,
       EXISTS (
           SELECT 1 FROM placement_copies pc
           WHERE pc.version_id = vp.version_id AND pc.state = 'reserved'
       ) AS migration_in_progress
FROM version_placements vp
JOIN versions v ON v.id = vp.version_id
JOIN objects o ON o.id = v.object_id
JOIN buckets b ON b.id = o.bucket_id`

const selectPlacementByVersion = selectPlacements + "\nWHERE vp.version_id = $1"

func (r *repo) Get(ctx context.Context, versionID string) (*model.Placement, error) {
	return r.get(ctx, db.Query{
		Name:     "placement_repository:Get",
		QueryRaw: selectPlacementByVersion,
	}, versionID)
}

func (r *repo) LockForChange(ctx context.Context, versionID string) (*model.Placement, error) {
	// FOR UPDATE re-reads locked rows after a concurrent commit, so is_current and
	// generation reflect the latest overwrite or switch.
	return r.get(ctx, db.Query{
		Name:     "placement_repository:LockForChange",
		QueryRaw: selectPlacementByVersion + "\nFOR UPDATE OF o, vp",
	}, versionID)
}

func (r *repo) get(ctx context.Context, q db.Query, versionID string) (*model.Placement, error) {
	var p repoModel.Placement
	if err := r.db.DB().ScanOneContext(ctx, &p, q, versionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domainerrors.ErrPlacementNotFound
		}
		return nil, err
	}

	return converter.PlacementToDomain(&p), nil
}

// ListCurrent pages through current versions of active objects by version_id.
func (r *repo) ListCurrent(ctx context.Context, afterVersionID string, limit int32) ([]*model.Placement, error) {
	q := db.Query{
		Name: "placement_repository:ListCurrent",
		QueryRaw: selectPlacements + `
WHERE o.current_version_id = v.id
  AND o.status = 'active'
  AND vp.version_id > COALESCE(NULLIF($1, '')::uuid, '00000000-0000-0000-0000-000000000000')
ORDER BY vp.version_id
LIMIT $2`,
	}

	var rows []repoModel.Placement
	if err := r.db.DB().ScanAllContext(ctx, &rows, q, afterVersionID, limit); err != nil {
		return nil, err
	}

	placements := make([]*model.Placement, 0, len(rows))
	for i := range rows {
		placements = append(placements, converter.PlacementToDomain(&rows[i]))
	}

	return placements, nil
}
