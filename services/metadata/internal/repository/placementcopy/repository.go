package placementcopy

import (
	"context"
	"errors"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/repository"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placementcopy/converter"
	repoModel "github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placementcopy/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/jackc/pgx/v4"
)

const copyColumns = `
       copy_id,
       migration_id,
       version_id,
       role::text AS role,
       storage_class,
       blob_id,
       size_bytes,
       expected_generation,
       expected_class,
       state::text AS state,
       verified_md5,
       cleanup_after,
       cleanup_attempts,
       last_error,
       created_at,
       updated_at`

type repo struct {
	db db.Client
}

func NewRepository(dbClient db.Client) repository.CopyRepository {
	return &repo{db: dbClient}
}

func (r *repo) scanOne(ctx context.Context, q db.Query, args ...any) (*model.PlacementCopy, error) {
	var c repoModel.PlacementCopy
	if err := r.db.DB().ScanOneContext(ctx, &c, q, args...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domainerrors.ErrCopyNotFound
		}
		return nil, err
	}

	return converter.CopyToDomain(&c), nil
}

func (r *repo) scanAll(ctx context.Context, q db.Query, args ...any) ([]*model.PlacementCopy, error) {
	var rows []repoModel.PlacementCopy
	if err := r.db.DB().ScanAllContext(ctx, &rows, q, args...); err != nil {
		return nil, err
	}

	return converter.CopiesToDomain(rows), nil
}
