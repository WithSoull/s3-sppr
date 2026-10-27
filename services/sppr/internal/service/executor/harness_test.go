package executor_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/executor"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
)

const (
	actionID     = "6c7f2a59-6a0a-4d55-9e0e-4a3f6f6b0001"
	versionID    = "6c7f2a59-6a0a-4d55-9e0e-4a3f6f6b0002"
	sourceBlobID = "6c7f2a59-6a0a-4d55-9e0e-4a3f6f6b0003"
	targetBlobID = "6c7f2a59-6a0a-4d55-9e0e-4a3f6f6b0004"
	maxAttempts  = 3
)

var (
	content    = []byte("hello world")
	size       = int64(len(content))
	contentMD5 = md5hex(content)
)

func md5hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

// running returns an action as a worker sees it right after Claim.
func running(step model.ActionStep, reason model.Reason, openCharge bool) *model.Action {
	return &model.Action{
		ActionSpec: model.ActionSpec{
			ActionID: actionID, PlanID: "6c7f2a59-6a0a-4d55-9e0e-4a3f6f6b0009", VersionID: versionID,
			ExpectedGeneration: 1, SourceClass: model.StorageClassCold, TargetClass: model.StorageClassHot, SizeBytes: size,
		},
		Status: model.ActionRunning, Step: step, Reason: reason, OpenCharge: openCharge,
	}
}

func placement(generation int64, updatedAt time.Time) model.Placement {
	return model.Placement{VersionID: versionID, Bucket: "bucket", Key: "key", IsCurrent: true,
		Class: model.StorageClassCold, BlobID: sourceBlobID, Generation: generation, SizeBytes: size, UpdatedAt: updatedAt}
}

func reserved() model.PlacementCopy {
	return model.PlacementCopy{CopyID: "copy", Role: model.CopyRoleTarget, Class: model.StorageClassHot, BlobID: targetBlobID,
		SizeBytes: size, ExpectedGeneration: 1, ExpectedClass: model.StorageClassCold, State: model.CopyReserved}
}

// step is the part of a Transition a test asserts; from-fields and error text
// follow from the order.
type step struct {
	Status model.ActionStatus
	Step   model.ActionStep
	Reason model.Reason
	Settle bool
	Bytes  int64
	Reopen bool
}

type deps struct {
	observer   service.MigrationObserver
	actions    *mocks.ActionRepositoryMock
	settings   *mocks.SettingsRepositoryMock
	pins       *mocks.PinRepositoryMock
	tx         *mocks.TxManagerMock
	placements *mocks.PlacementClientMock
	hot, cold  *mocks.StorageClientMock
	recorded   []step
}

// newDeps hands a to the next Step as an interrupted running action and records
// the transitions it makes.
func newDeps(mc *minimock.Controller, a *model.Action) *deps {
	d := &deps{
		actions:    mocks.NewActionRepositoryMock(mc),
		settings:   mocks.NewSettingsRepositoryMock(mc),
		pins:       mocks.NewPinRepositoryMock(mc),
		tx:         mocks.NewTxManagerMock(mc),
		placements: mocks.NewPlacementClientMock(mc),
		hot:        mocks.NewStorageClientMock(mc),
		cold:       mocks.NewStorageClientMock(mc),
	}
	d.tx.ReadCommittedMock.Optional().Set(func(ctx context.Context, f db.Handler) error { return f(ctx) })
	if a != nil {
		d.actions.ListRunningMock.Return([]*model.Action{a}, nil)
	}
	d.actions.TransitionMock.Optional().Set(func(_ context.Context, t model.Transition) ([]string, error) {
		d.recorded = append(d.recorded, step{t.Status, t.Step, t.Reason, t.Settle, t.Bytes, t.Reopen})
		return nil, nil
	})
	return d
}

func (d *deps) executor() service.MigrationExecutor {
	return executor.NewExecutor(executor.Config{
		Workers: 1, MaxAttempts: maxAttempts, RetryBackoff: time.Millisecond,
		SourceCleanupDelay: 10 * time.Minute, PollInterval: time.Millisecond,
	}, d.actions, d.settings, d.pins, d.tx, d.placements,
		map[model.StorageClass]client.StorageClient{model.StorageClassHot: d.hot, model.StorageClassCold: d.cold}, d.observer)
}

func (d *deps) checks(pinned bool, minInterval time.Duration) {
	d.pins.IsPinnedMock.Expect(minimock.AnyContext, "bucket", "key").Return(pinned, nil)
	if !pinned {
		d.settings.GetMock.Return(model.MigrationSettings{MinInterval: minInterval}, nil)
	}
}

// source serves content from COLD on every attempt.
func (d *deps) source(data []byte) {
	d.cold.OpenMock.Set(func(_ context.Context, blobID string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})
}

// sink stores into HOT, answering attempt i with results[i] (the last repeats).
func (d *deps) sink(results ...error) {
	attempt := 0
	d.hot.StoreMock.Set(func(_ context.Context, blobID string, _ int64, body io.Reader) (string, error) {
		got, _ := io.ReadAll(body)
		err := results[min(attempt, len(results)-1)]
		attempt++
		if err != nil {
			return "", err
		}
		return md5hex(got), nil
	})
}

func unavailable() error { return model.ErrUpstreamUnavailable }

func fresh(d *deps) {
	d.placements.ListMigrationCopiesMock.Expect(minimock.AnyContext, actionID).Return(nil, nil)
}

func switches(d *deps, err error) {
	d.placements.SwitchPlacementMock.Expect(minimock.AnyContext, model.SwitchRequest{
		MigrationID: actionID, VerifiedSizeBytes: size, VerifiedMD5: contentMD5, SourceCleanupDelay: 10 * time.Minute,
	}).Return(err)
}

func aborts(d *deps, reason model.Reason, err error) {
	d.placements.AbortCopyMock.Expect(minimock.AnyContext, actionID, string(reason)).Return(err)
}

func copying() step { return step{Status: model.ActionRunning, Step: model.StepCopying} }
func switching() step {
	return step{Status: model.ActionRunning, Step: model.StepSwitching, Settle: true, Bytes: size}
}
func succeeded() step { return step{Status: model.ActionSucceeded} }
func aborting(reason model.Reason, bytes int64) step {
	return step{Status: model.ActionRunning, Step: model.StepAborting, Reason: reason, Settle: true, Bytes: bytes}
}
func failed(reason model.Reason) step { return step{Status: model.ActionFailed, Reason: reason} }

// refunded ends an action before any copy attempt.
func refunded(status model.ActionStatus, reason model.Reason) step {
	return step{Status: status, Reason: reason, Settle: true}
}

func TestMain(m *testing.M) {
	logger.SetNopLogger()
	os.Exit(m.Run())
}
