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

const selectPlacement = `
SELECT vp.version_id,
       o.id AS object_id,
       b.name AS bucket_name,
       o.key,
       COALESCE(o.current_version_id = v.id AND o.status = 'active', false) AS is_current,
       vp.storage_class,
       vp.blob_id,
       vp.generation,
       v.size_bytes,
       vp.updated_at
FROM version_placements vp
JOIN versions v ON v.id = vp.version_id
JOIN objects o ON o.id = v.object_id
JOIN buckets b ON b.id = o.bucket_id
WHERE vp.version_id = $1`

func (r *repo) Get(ctx context.Context, versionID string) (*model.Placement, error) {
	return r.get(ctx, db.Query{
		Name:     "placement_repository:Get",
		QueryRaw: selectPlacement,
	}, versionID)
}

func (r *repo) LockForChange(ctx context.Context, versionID string) (*model.Placement, error) {
	// FOR UPDATE re-reads locked rows after a concurrent commit, so is_current and
	// generation reflect the latest overwrite or switch.
	return r.get(ctx, db.Query{
		Name:     "placement_repository:LockForChange",
		QueryRaw: selectPlacement + "\nFOR UPDATE OF o, vp",
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
