package migration

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

func (s *migrationService) SetPaused(ctx context.Context, paused bool) error {
	return s.updateSettings(ctx, func(settings *model.MigrationSettings) { settings.Paused = paused })
}

func (s *migrationService) GetBudget(ctx context.Context) (model.BudgetStatus, error) {
	settings, err := s.settings.Get(ctx)
	if err != nil {
		return model.BudgetStatus{}, err
	}
	spent, err := s.settings.Spent(ctx, settings.BudgetWindow)
	if err != nil {
		return model.BudgetStatus{}, err
	}
	return model.BudgetStatus{
		Bytes:     settings.BudgetBytes,
		Window:    settings.BudgetWindow,
		Spent:     spent,
		Remaining: max(settings.BudgetBytes-spent, 0),
	}, nil
}

func (s *migrationService) SetBudget(ctx context.Context, bytes int64, window time.Duration) error {
	if bytes <= 0 {
		return invalid("bytes must be positive")
	}
	if window < time.Second || window%time.Second != 0 {
		return invalid("window_seconds must be a positive number of seconds")
	}
	return s.updateSettings(ctx, func(settings *model.MigrationSettings) {
		settings.BudgetBytes, settings.BudgetWindow = bytes, window
	})
}

func (s *migrationService) GetSettings(ctx context.Context) (model.MigrationSettings, error) {
	return s.settings.Get(ctx)
}

func (s *migrationService) SetMinInterval(ctx context.Context, interval time.Duration) error {
	if interval < 0 || interval%time.Second != 0 {
		return invalid("min_interval_seconds must be a non-negative number of seconds")
	}
	return s.updateSettings(ctx, func(settings *model.MigrationSettings) { settings.MinInterval = interval })
}

// updateSettings locks the row so a change does not overwrite a concurrent one.
func (s *migrationService) updateSettings(ctx context.Context, change func(*model.MigrationSettings)) error {
	return s.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		settings, err := s.settings.GetForUpdate(ctx)
		if err != nil {
			return err
		}
		change(&settings)
		return s.settings.Update(ctx, settings)
	})
}
