package placement

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placement/converter"
	repoModel "github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placement/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

// Occupied space: placements of every version (non-current ones too) plus copies
// that still hold a blob in storage.
const selectUsage = `
SELECT sc.name,
       sc.capacity_bytes,
       sc.max_utilization::float8 AS max_utilization,
       floor(sc.capacity_bytes * sc.max_utilization)::bigint AS usable_bytes,
       COALESCE(p.bytes, 0) AS placed_bytes,
       COALESCE(p.versions, 0) AS placed_versions,
       COALESCE(c.bytes, 0) AS copy_bytes,
       COALESCE(c.copies, 0) AS copies,
       COALESCE(c.failed, 0) AS failed_cleanups
FROM storage_classes sc
LEFT JOIN (
    SELECT vp.storage_class, SUM(v.size_bytes)::bigint AS bytes, COUNT(*) AS versions
    FROM version_placements vp
    JOIN versions v ON v.id = vp.version_id
    GROUP BY vp.storage_class
) p ON p.storage_class = sc.name
LEFT JOIN (
    SELECT storage_class,
           SUM(size_bytes)::bigint AS bytes,
           COUNT(*) AS copies,
           COUNT(*) FILTER (WHERE state = 'cleanup_failed') AS failed
    FROM placement_copies
    WHERE state IN ('reserved', 'cleanup_pending', 'cleanup_failed')
    GROUP BY storage_class
) c ON c.storage_class = sc.name`

func (r *repo) LockStorageClass(ctx context.Context, storageClass model.StorageClass) error {
	q := db.Query{
		Name:     "placement_repository:LockStorageClass",
		QueryRaw: "SELECT name FROM storage_classes WHERE name = $1 FOR UPDATE",
	}

	var name string
	return r.db.DB().QueryRowContext(ctx, q, storageClass).Scan(&name)
}

// GetUsage must run as a separate statement after LockStorageClass: under READ
// COMMITTED it then sees copies reserved by the transaction that held the lock.
func (r *repo) GetUsage(ctx context.Context, storageClass model.StorageClass) (*model.StorageClassUsage, error) {
	q := db.Query{
		Name:     "placement_repository:GetUsage",
		QueryRaw: selectUsage + "\nWHERE sc.name = $1",
	}

	var u repoModel.StorageClassUsage
	if err := r.db.DB().ScanOneContext(ctx, &u, q, storageClass); err != nil {
		return nil, err
	}

	return converter.UsageToDomain(&u), nil
}
