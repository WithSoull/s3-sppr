package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

// process continues a running action from the state Metadata recorded for its
// migration_id, so a fresh claim and a resume after a restart share one path.
func (e *executor) process(ctx context.Context, a *model.Action) error {
	if a.Step == model.StepAborting {
		return e.abort(ctx, a)
	}
	var copies []model.PlacementCopy
	err := e.retry(ctx, func() error {
		var err error
		copies, err = e.placements.ListMigrationCopies(ctx, a.ActionID)
		return err
	})
	if err != nil {
		return e.failAndAbort(ctx, a, upstreamReason(err), unknownCharge(a), err)
	}
	var target *model.PlacementCopy
	for i := range copies {
		if copies[i].Role == model.CopyRoleTarget {
			target = &copies[i]
		}
	}
	switch {
	case target == nil:
		return e.start(ctx, a)
	case target.State == model.CopyReserved:
		return e.copyAndSwitch(ctx, a, *target, nil)
	case target.State == model.CopyPromoted:
		return e.move(ctx, a, model.Transition{Status: model.ActionSucceeded, Settle: true, Bytes: unknownCharge(a)})
	default:
		// The copy was aborted before the restart; keep the reason recorded then.
		reason := a.Reason
		if reason == model.ReasonNone {
			reason = model.ReasonInternal
		}
		return e.move(ctx, a, model.Transition{Status: model.ActionFailed, Reason: reason,
			Settle: true, Bytes: unknownCharge(a), Error: "target copy is " + string(target.State)})
	}
}

// start runs the checks that precede a reservation; nothing is copied yet, so
// every outcome here returns the charge.
func (e *executor) start(ctx context.Context, a *model.Action) error {
	var p model.Placement
	err := e.retry(ctx, func() error {
		var err error
		p, err = e.placements.GetPlacement(ctx, a.VersionID)
		return err
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		reason := upstreamReason(err)
		if errors.Is(err, model.ErrUpstreamNotFound) {
			reason = model.ReasonStale
		}
		return e.end(ctx, a, model.ActionFailed, reason, err)
	}
	pinned, err := e.pins.IsPinned(ctx, p.Bucket, p.Key)
	if err != nil {
		return fmt.Errorf("check pin: %w", err)
	}
	if pinned {
		return e.end(ctx, a, model.ActionCancelled, model.ReasonPinned, nil)
	}
	settings, err := e.settings.Get(ctx)
	if err != nil {
		return fmt.Errorf("read settings: %w", err)
	}
	// The first placement was chosen at write time, not by a migration.
	if p.Generation > 1 && time.Since(p.UpdatedAt) < settings.MinInterval {
		return e.end(ctx, a, model.ActionCancelled, model.ReasonMinInterval, nil)
	}

	var target model.PlacementCopy
	err = e.retry(ctx, func() error {
		var err error
		target, err = e.placements.ReserveCopy(ctx, a.ActionSpec)
		return err
	})
	switch {
	case err == nil:
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, model.ErrStale), errors.Is(err, model.ErrUpstreamNotFound):
		return e.end(ctx, a, model.ActionFailed, model.ReasonStale, err)
	case errors.Is(err, model.ErrNoCapacity):
		return e.end(ctx, a, model.ActionFailed, model.ReasonNoCapacity, err)
	case errors.Is(err, model.ErrUpstreamPrecondition):
		return e.end(ctx, a, model.ActionFailed, model.ReasonInternal, err)
	default:
		// The reservation may have been made before the error reached us.
		return e.failAndAbort(ctx, a, upstreamReason(err), 0, err)
	}
	source := &p
	if p.Generation != target.ExpectedGeneration || p.Class != target.ExpectedClass {
		source = nil
	}
	return e.copyAndSwitch(ctx, a, target, source)
}

func (e *executor) copyAndSwitch(ctx context.Context, a *model.Action, target model.PlacementCopy, p *model.Placement) error {
	if target.SizeBytes != a.SizeBytes {
		return e.failAndAbort(ctx, a, model.ReasonStale, unknownCharge(a),
			fmt.Errorf("planned size_bytes %d differs from reserved copy size_bytes %d", a.SizeBytes, target.SizeBytes))
	}
	if p == nil {
		var fresh model.Placement
		err := e.retry(ctx, func() error {
			var err error
			fresh, err = e.placements.GetPlacement(ctx, a.VersionID)
			return err
		})
		if err != nil {
			reason := upstreamReason(err)
			if errors.Is(err, model.ErrUpstreamNotFound) {
				reason = model.ReasonStale
			}
			return e.failAndAbort(ctx, a, reason, unknownCharge(a), err)
		}
		p = &fresh
	}
	// The copy must be of the blob that was placed when the reservation was made.
	if !p.IsCurrent || p.Generation != target.ExpectedGeneration || p.Class != target.ExpectedClass {
		return e.failAndAbort(ctx, a, model.ReasonStale, unknownCharge(a), errors.New("placement changed since the reservation"))
	}
	source, sink := e.storages[p.Class], e.storages[target.Class]
	if source == nil || sink == nil {
		return e.failAndAbort(ctx, a, model.ReasonInternal, unknownCharge(a), fmt.Errorf("no Storage client for %s -> %s", p.Class, target.Class))
	}
	if err := e.enterCopying(ctx, a); err != nil {
		return err
	}

	var copied copyResult
	backoff := e.cfg.RetryBackoff
	for attempt := 1; ; attempt++ {
		copied = copyBlob(ctx, source, p.BlobID, sink, target.BlobID, p.SizeBytes)
		if e.observer != nil {
			e.observer.Copied(ctx, string(target.Class), copied.bytes)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		reason := copied.failure(p.SizeBytes)
		if reason == model.ReasonNone {
			break
		}
		if reason == model.ReasonUpstreamUnavailable && attempt < e.cfg.MaxAttempts {
			// Bytes of a failed attempt stay charged; the next attempt is charged anew.
			if err := e.move(ctx, a, model.Transition{Status: model.ActionRunning, Step: model.StepCopying,
				Settle: true, Bytes: copied.bytes, Reopen: true, Error: copied.err.Error()}); err != nil {
				return err
			}
			if err := sleep(ctx, backoff); err != nil {
				return err
			}
			backoff *= 2
			continue
		}
		return e.failAndAbort(ctx, a, reason, copied.bytes, copied.describe(p.SizeBytes))
	}
	if err := e.move(ctx, a, model.Transition{Status: model.ActionRunning, Step: model.StepSwitching,
		Settle: true, Bytes: copied.bytes}); err != nil {
		return err
	}

	err := e.retry(ctx, func() error {
		return e.placements.SwitchPlacement(ctx, model.SwitchRequest{
			MigrationID:        a.ActionID,
			VerifiedSizeBytes:  copied.bytes,
			VerifiedMD5:        copied.sourceMD5,
			SourceCleanupDelay: e.cfg.SourceCleanupDelay,
		})
	})
	switch {
	case err == nil:
		return e.move(ctx, a, model.Transition{Status: model.ActionSucceeded})
	case errors.Is(err, model.ErrStale):
		return e.failAndAbort(ctx, a, model.ReasonStale, 0, err)
	default:
		return e.failAndAbort(ctx, a, upstreamReason(err), 0, err)
	}
}

// enterCopying prepares the budget charge for the first copy attempt of this run.
func (e *executor) enterCopying(ctx context.Context, a *model.Action) error {
	t := model.Transition{Status: model.ActionRunning, Step: model.StepCopying}
	switch {
	case a.Step == model.StepCopying:
		// An attempt was interrupted; its transferred bytes are unknown, so the
		// planned size stays charged.
		t.Settle, t.Bytes, t.Reopen = true, a.SizeBytes, true
	case !a.OpenCharge:
		t.Reopen = true
	}
	return e.move(ctx, a, t)
}

// end finishes an action before any copy attempt and returns its charge.
func (e *executor) end(ctx context.Context, a *model.Action, status model.ActionStatus, reason model.Reason, cause error) error {
	t := model.Transition{Status: status, Reason: reason, Settle: true, Bytes: 0}
	if cause != nil {
		t.Error = cause.Error()
	}
	return e.move(ctx, a, t)
}

// failAndAbort records the reason before AbortCopy, so that a restart finishes
// the abort with the same reason instead of copying again.
func (e *executor) failAndAbort(ctx context.Context, a *model.Action, reason model.Reason, transferred int64, cause error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := e.move(ctx, a, model.Transition{Status: model.ActionRunning, Step: model.StepAborting, Reason: reason,
		Settle: true, Bytes: transferred, Error: cause.Error()}); err != nil {
		return err
	}
	return e.abort(ctx, a)
}

func (e *executor) abort(ctx context.Context, a *model.Action) error {
	err := e.retry(ctx, func() error { return e.placements.AbortCopy(ctx, a.ActionID, string(a.Reason)) })
	switch {
	case err == nil, errors.Is(err, model.ErrUpstreamNotFound):
		return e.move(ctx, a, model.Transition{Status: model.ActionFailed, Reason: a.Reason})
	case errors.Is(err, model.ErrUpstreamPrecondition):
		// AbortCopy refuses only after SwitchPlacement: a switch whose reply was
		// lost did happen.
		return e.move(ctx, a, model.Transition{Status: model.ActionSucceeded, Error: err.Error()})
	default:
		return fmt.Errorf("abort copy of action %s: %w", a.ActionID, err)
	}
}

// unknownCharge is the charge for an attempt interrupted before this run.
func unknownCharge(a *model.Action) int64 {
	if a.Step == model.StepCopying {
		return a.SizeBytes
	}
	return 0
}

func upstreamReason(err error) model.Reason {
	if client.IsTransient(err) {
		return model.ReasonUpstreamUnavailable
	}
	return model.ReasonInternal
}
