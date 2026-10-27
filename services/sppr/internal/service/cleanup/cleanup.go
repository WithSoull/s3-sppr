package cleanup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

const batchLimit = 100
const deleteTimeout = 5 * time.Second

type cleanup struct {
	interval   time.Duration
	actions    repository.ActionRepository
	placements client.PlacementClient
	storages   map[model.StorageClass]client.StorageClient
	observer   service.MigrationObserver
}

func NewService(interval time.Duration, actions repository.ActionRepository, placements client.PlacementClient, storages map[model.StorageClass]client.StorageClient, observer service.MigrationObserver) service.MigrationCleanup {
	return &cleanup{interval: interval, actions: actions, placements: placements, storages: storages, observer: observer}
}

func (s *cleanup) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if err := s.Step(ctx); err != nil && ctx.Err() == nil {
			logger.Error(ctx, "migration cleanup failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *cleanup) Step(ctx context.Context) error {
	copies, err := s.placements.ListDueCleanups(ctx, batchLimit)
	if err != nil {
		return fmt.Errorf("list due cleanups: %w", err)
	}
	var result error
	for _, copy := range copies {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		result = errors.Join(result, s.process(ctx, copy))
	}
	if ctx.Err() != nil {
		return errors.Join(result, ctx.Err())
	}
	return errors.Join(result, s.releaseDependents(ctx))
}

// releaseDependents marks actions whose source blob Metadata reports deleted,
// so the actions waiting for the space they free can be claimed. It asks
// Metadata rather than trusting this loop's own deletions, which a restart or
// a lost CompleteCleanup response would lose.
func (s *cleanup) releaseDependents(ctx context.Context) error {
	ids, err := s.actions.ListAwaitingSourceCleanup(ctx, batchLimit)
	if err != nil {
		return fmt.Errorf("list actions awaiting source cleanup: %w", err)
	}
	var result error
	for _, id := range ids {
		copies, err := s.placements.ListMigrationCopies(ctx, id)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("list copies of %s: %w", id, err))
			continue
		}
		if !sourceDeleted(copies) {
			continue
		}
		if err := s.actions.MarkSourceCleaned(ctx, id); err != nil {
			result = errors.Join(result, fmt.Errorf("mark source of %s cleaned: %w", id, err))
		}
	}
	return result
}

func sourceDeleted(copies []model.PlacementCopy) bool {
	for _, c := range copies {
		if c.Role == model.CopyRoleSource && c.State != model.CopyDeleted {
			return false
		}
	}
	return true
}

func (s *cleanup) process(ctx context.Context, copy model.PlacementCopy) error {
	storage := s.storages[copy.Class]
	var deleteErr error
	if storage == nil {
		deleteErr = fmt.Errorf("no Storage client for %s", copy.Class)
	} else {
		callCtx, cancel := context.WithTimeout(ctx, deleteTimeout)
		deleteErr = storage.Delete(callCtx, copy.BlobID)
		cancel()
	}
	// Shutdown is not evidence of a Storage failure; leave the copy due for recovery.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	message, outcome := "", "deleted"
	if deleteErr != nil {
		message, outcome = deleteErr.Error(), "delete_failed"
	}
	completeErr := s.placements.CompleteCleanup(ctx, copy.CopyID, deleteErr == nil, message)
	if completeErr != nil {
		outcome = "completion_failed"
	}
	if s.observer != nil {
		s.observer.Cleanup(ctx, outcome)
	}
	logger.Info(ctx, "migration cleanup attempt", zap.String("copy_id", copy.CopyID), zap.String("blob_id", copy.BlobID),
		zap.String("storage_class", string(copy.Class)), zap.String("role", string(copy.Role)), zap.String("outcome", outcome),
		zap.NamedError("delete_error", deleteErr), zap.NamedError("completion_error", completeErr))
	return errors.Join(deleteErr, completeErr)
}
