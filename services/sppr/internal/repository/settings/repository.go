package settings

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/contextx/txctx"
	"github.com/jackc/pgx/v4"
)

type repo struct{ db db.Client }

func NewRepository(client db.Client) repository.SettingsRepository { return &repo{db: client} }

const selectSettings = `SELECT budget_bytes, budget_window_seconds, min_interval_seconds, paused FROM migration_settings`

func (r *repo) EnsureDefaults(ctx context.Context, s model.MigrationSettings) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "settings:defaults", QueryRaw: `
		INSERT INTO migration_settings (budget_bytes, budget_window_seconds, min_interval_seconds, paused)
		VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`},
		s.BudgetBytes, seconds(s.BudgetWindow), seconds(s.MinInterval), s.Paused)
	return err
}

func (r *repo) Get(ctx context.Context) (model.MigrationSettings, error) {
	return r.scan(ctx, db.Query{Name: "settings:get", QueryRaw: selectSettings})
}

func (r *repo) GetForUpdate(ctx context.Context) (model.MigrationSettings, error) {
	if _, ok := txctx.ExtractTx(ctx); !ok {
		return model.MigrationSettings{}, fmt.Errorf("settings lock requires a transaction")
	}
	return r.scan(ctx, db.Query{Name: "settings:get_for_update", QueryRaw: selectSettings + ` FOR UPDATE`})
}

func (r *repo) scan(ctx context.Context, q db.Query) (model.MigrationSettings, error) {
	var budget, window, minInterval int64
	var paused bool
	err := r.db.DB().QueryRowContext(ctx, q).Scan(&budget, &window, &minInterval, &paused)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.MigrationSettings{}, fmt.Errorf("migration settings are not initialized")
	}
	if err != nil {
		return model.MigrationSettings{}, err
	}
	return model.MigrationSettings{
		BudgetBytes:  budget,
		BudgetWindow: time.Duration(window) * time.Second,
		MinInterval:  time.Duration(minInterval) * time.Second,
		Paused:       paused,
	}, nil
}

func (r *repo) Update(ctx context.Context, s model.MigrationSettings) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "settings:update", QueryRaw: `
		UPDATE migration_settings
		SET budget_bytes = $1, budget_window_seconds = $2, min_interval_seconds = $3, paused = $4, updated_at = now()`},
		s.BudgetBytes, seconds(s.BudgetWindow), seconds(s.MinInterval), s.Paused)
	return err
}

func (r *repo) Spent(ctx context.Context, window time.Duration) (int64, error) {
	var spent int64
	err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "settings:spent", QueryRaw: `
		SELECT COALESCE(sum(bytes), 0)::bigint FROM migration_budget_ledger
		WHERE charged_at > now() - $1::bigint * interval '1 millisecond'`}, window.Milliseconds()).Scan(&spent)
	return spent, err
}

func seconds(d time.Duration) int64 { return int64(d / time.Second) }
