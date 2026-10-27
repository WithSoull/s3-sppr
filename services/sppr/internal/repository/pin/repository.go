package pin

import (
	"context"
	"errors"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/jackc/pgx/v4"
)

type repo struct{ db db.Client }

func NewRepository(client db.Client) repository.PinRepository { return &repo{db: client} }

type pinRow struct {
	Bucket    string    `db:"bucket"`
	Key       string    `db:"object_key"`
	Reason    string    `db:"reason"`
	Author    string    `db:"author"`
	CreatedAt time.Time `db:"created_at"`
}

func (p pinRow) toModel() model.Pin {
	return model.Pin{Bucket: p.Bucket, Key: p.Key, Reason: p.Reason, Author: p.Author, CreatedAt: p.CreatedAt.UTC()}
}

func (r *repo) List(ctx context.Context) ([]model.Pin, error) {
	var rows []pinRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "pin:list", QueryRaw: `
		SELECT bucket, object_key, reason, author, created_at FROM migration_pins ORDER BY bucket, object_key`}); err != nil {
		return nil, err
	}
	pins := make([]model.Pin, 0, len(rows))
	for _, row := range rows {
		pins = append(pins, row.toModel())
	}
	return pins, nil
}

func (r *repo) Create(ctx context.Context, p model.Pin) (model.Pin, error) {
	var createdAt time.Time
	err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "pin:create", QueryRaw: `
		INSERT INTO migration_pins (bucket, object_key, reason, author) VALUES ($1, $2, $3, $4)
		ON CONFLICT (bucket, object_key) DO NOTHING
		RETURNING created_at`}, p.Bucket, p.Key, p.Reason, p.Author).Scan(&createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Pin{}, model.ErrPinExists
	}
	if err != nil {
		return model.Pin{}, err
	}
	p.CreatedAt = createdAt.UTC()
	return p, nil
}

func (r *repo) Delete(ctx context.Context, bucket, key string) error {
	tag, err := r.db.DB().ExecContext(ctx, db.Query{Name: "pin:delete", QueryRaw: `
		DELETE FROM migration_pins WHERE bucket = $1 AND object_key = $2`}, bucket, key)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return model.ErrPinNotFound
	}
	return nil
}

func (r *repo) IsPinned(ctx context.Context, bucket, key string) (bool, error) {
	var pinned bool
	err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "pin:exists", QueryRaw: `
		SELECT EXISTS (SELECT 1 FROM migration_pins WHERE bucket = $1 AND object_key = $2)`}, bucket, key).Scan(&pinned)
	return pinned, err
}
