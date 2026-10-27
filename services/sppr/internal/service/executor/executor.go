package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

type Config struct {
	Workers      int
	MaxAttempts  int
	RetryBackoff time.Duration
	// SourceCleanupDelay must exceed the Gateway read limit: a GET that resolved
	// the old placement may still be streaming the source blob.
	SourceCleanupDelay time.Duration
	PollInterval       time.Duration
}

type executor struct {
	cfg        Config
	actions    repository.ActionRepository
	settings   repository.SettingsRepository
	pins       repository.PinRepository
	tx         db.TxManager
	placements client.PlacementClient
	storages   map[model.StorageClass]client.StorageClient

	mu sync.Mutex
	// held are the actions this process is working on; the rest of the running
	// ones were interrupted and may be resumed.
	held map[string]struct{}
}

func NewExecutor(
	cfg Config,
	actions repository.ActionRepository,
	settings repository.SettingsRepository,
	pins repository.PinRepository,
	tx db.TxManager,
	placements client.PlacementClient,
	storages map[model.StorageClass]client.StorageClient,
) service.MigrationExecutor {
	return &executor{
		cfg:        cfg,
		actions:    actions,
		settings:   settings,
		pins:       pins,
		tx:         tx,
		placements: placements,
		storages:   storages,
		held:       map[string]struct{}{},
	}
}

func (e *executor) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < e.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.work(ctx)
		}()
	}
	wg.Wait()
	return nil
}

func (e *executor) work(ctx context.Context) {
	for ctx.Err() == nil {
		worked, err := e.Step(ctx)
		if err != nil && ctx.Err() == nil {
			logger.Error(ctx, "migration step failed", zap.Error(err))
		}
		if worked && err == nil {
			continue
		}
		_ = sleep(ctx, e.cfg.PollInterval)
	}
}

func (e *executor) Step(ctx context.Context) (bool, error) {
	a, err := e.next(ctx)
	if err != nil || a == nil {
		return false, err
	}
	defer e.release(a.ActionID)
	return true, e.process(ctx, a)
}

// next resumes an interrupted running action first: paused or over budget,
// started work is still finished.
func (e *executor) next(ctx context.Context) (*model.Action, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	running, err := e.actions.ListRunning(ctx)
	if err != nil {
		return nil, fmt.Errorf("list running actions: %w", err)
	}
	for _, a := range running {
		if _, ok := e.held[a.ActionID]; !ok {
			e.held[a.ActionID] = struct{}{}
			return a, nil
		}
	}
	var claimed *model.Action
	err = e.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		settings, err := e.settings.GetForUpdate(ctx)
		if err != nil || settings.Paused {
			return err
		}
		spent, err := e.settings.Spent(ctx, settings.BudgetWindow)
		if err != nil {
			return err
		}
		claimed, err = e.actions.Claim(ctx, settings.BudgetBytes-spent)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("claim action: %w", err)
	}
	if claimed != nil {
		e.held[claimed.ActionID] = struct{}{}
		logger.Info(ctx, "migration action claimed", zap.String("action_id", claimed.ActionID),
			zap.String("version_id", claimed.VersionID), zap.Int64("size_bytes", claimed.SizeBytes))
	}
	return claimed, nil
}

func (e *executor) release(actionID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.held, actionID)
}

// move records a transition of a and applies it to the in-memory copy.
func (e *executor) move(ctx context.Context, a *model.Action, t model.Transition) error {
	t.ActionID, t.FromStatus, t.FromStep = a.ActionID, a.Status, a.Step
	var dependents []string
	err := e.tx.ReadCommitted(ctx, func(ctx context.Context) error {
		var err error
		dependents, err = e.actions.Transition(ctx, t)
		return err
	})
	if err != nil {
		return fmt.Errorf("record %s/%s of action %s: %w", t.Status, t.Step, a.ActionID, err)
	}
	a.Status, a.Step, a.Reason = t.Status, t.Step, t.Reason
	if t.Settle {
		a.OpenCharge = false
	}
	if t.Reopen {
		a.OpenCharge = true
	}
	logger.Info(ctx, "migration action transition",
		zap.String("action_id", a.ActionID), zap.String("version_id", a.VersionID),
		zap.String("from", string(t.FromStatus)+"/"+string(t.FromStep)),
		zap.String("to", string(t.Status)+"/"+string(t.Step)),
		zap.String("reason", string(t.Reason)), zap.String("error", t.Error),
		zap.Strings("cancelled_dependents", dependents))
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// retry repeats f while Metadata or Storage is unavailable, up to MaxAttempts.
func (e *executor) retry(ctx context.Context, f func() error) error {
	backoff := e.cfg.RetryBackoff
	for attempt := 1; ; attempt++ {
		err := f()
		if err == nil || !client.IsTransient(err) || attempt >= e.cfg.MaxAttempts {
			return err
		}
		if err := sleep(ctx, backoff); err != nil {
			return err
		}
		backoff *= 2
	}
}
