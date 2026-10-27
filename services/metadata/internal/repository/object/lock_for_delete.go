package object

import (
	"context"
	"errors"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/jackc/pgx/v4"
)

// LockForDelete locks the object row until the transaction ends. Overwrites and
// placement switches lock the same row, so the versions and placements read after
// the lock stay complete until the object is deleted.
func (r *repo) LockForDelete(ctx context.Context, bucketName, key string) (string, error) {
	rawSQL := `
SELECT o.id
FROM objects o
JOIN buckets b ON b.id = o.bucket_id
WHERE b.name = $1 AND o.key = $2
FOR UPDATE OF o`

	q := db.Query{
		Name:     "object_repository:LockForDelete",
		QueryRaw: rawSQL,
	}

	var objectID string
	err := r.db.DB().QueryRowContext(ctx, q, bucketName, key).Scan(&objectID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", domainerrors.ErrObjectNotFound
		}
		return "", err
	}

	return objectID, nil
}
