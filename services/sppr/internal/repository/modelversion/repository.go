package modelversion

import (
	"context"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

type repo struct {
	db db.Client
	tx db.TxManager
}

func New(client db.Client, tx db.TxManager) repository.ModelVersionRepository {
	return &repo{client, tx}
}
func (r *repo) List(ctx context.Context, before time.Time, limit int) ([]model.ModelVersion, error) {
	if limit < 1 || limit > 100 {
		limit = 100
	}
	var rows []model.ModelVersion
	err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "models:list", QueryRaw: `SELECT * FROM model_versions
 WHERE ($1::timestamptz IS NULL OR created_at < $1) ORDER BY created_at DESC, model_version DESC LIMIT $2`}, nullableTime(before), limit)
	return rows, err
}
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
func (r *repo) Save(ctx context.Context, v model.ModelVersion) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "models:save", QueryRaw: `INSERT INTO model_versions
 (model_version,status,reason,source,trained_from,trained_to,train_rows,train_seconds,validation_cov_k,validation_ewma_cov_k,created_at,activated_at,retired_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
 ON CONFLICT (model_version) DO UPDATE SET status=EXCLUDED.status,reason=EXCLUDED.reason,
 trained_from=EXCLUDED.trained_from,trained_to=EXCLUDED.trained_to,train_rows=EXCLUDED.train_rows,
 train_seconds=EXCLUDED.train_seconds,validation_cov_k=EXCLUDED.validation_cov_k,
 validation_ewma_cov_k=EXCLUDED.validation_ewma_cov_k
 WHERE model_versions.status IN ('candidate','rejected')`}, v.ModelVersion, v.Status, v.Reason, v.Source, v.TrainedFrom, v.TrainedTo, v.TrainRows, v.TrainSeconds, v.ValidationCovK, v.ValidationEWMACovK, v.CreatedAt, v.ActivatedAt, v.RetiredAt)
	return err
}
func (r *repo) Activate(ctx context.Context, version string, at time.Time) error {
	return r.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "models:lock", QueryRaw: `LOCK TABLE model_versions IN EXCLUSIVE MODE`}); err != nil {
			return err
		}
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "models:retire_previous", QueryRaw: `UPDATE model_versions SET status='retired',reason='replaced',retired_at=$2 WHERE status='active' AND model_version<>$1`}, version, at); err != nil {
			return err
		}
		result, err := r.db.DB().ExecContext(ctx, db.Query{Name: "models:activate", QueryRaw: `UPDATE model_versions SET status='active',reason='',activated_at=COALESCE(activated_at,$2),retired_at=NULL WHERE model_version=$1 AND status IN ('candidate','active')`}, version, at)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return fmt.Errorf("model candidate missing: %s", version)
		}
		return nil
	})
}
func (r *repo) Retire(ctx context.Context, version, reason string, at time.Time) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "models:retire", QueryRaw: `UPDATE model_versions SET status='retired',reason=$2,retired_at=$3 WHERE model_version=$1 AND status='active'`}, version, reason, at)
	return err
}
func (r *repo) FailInterrupted(ctx context.Context) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "models:interrupt", QueryRaw: `UPDATE model_versions SET status='rejected',reason='training_interrupted' WHERE status='candidate'`})
	return err
}

func (r *repo) Rename(ctx context.Context, old, version string) error {
	result, err := r.db.DB().ExecContext(ctx, db.Query{Name: "models:rename", QueryRaw: `UPDATE model_versions SET model_version=$2 WHERE model_version=$1 AND status='candidate'`}, old, version)
	if err == nil && result.RowsAffected() != 1 {
		return fmt.Errorf("model attempt missing: %s", old)
	}
	return err
}
