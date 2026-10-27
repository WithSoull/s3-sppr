//go:build integration

package executor_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStandCorruptedCopy(t *testing.T) {
	s := newStand(t)
	before := s.placement()
	hot := s.storages[model.StorageClassHot]
	s.storages[model.StorageClassHot] = &storageFault{StorageClient: hot, store: func(ctx context.Context, id string, size int64, body io.Reader) (string, error) {
		return hot.Store(ctx, id, size, &corruptReader{Reader: body})
	}}
	s.submit()
	s.step()
	s.outcome(model.ActionFailed, model.ReasonVerifyMismatch)
	require.Equal(t, before.BlobID, s.placement().BlobID)
	require.Equal(t, "cleanup_pending", s.target().GetState())
	target := s.target()
	require.NoError(t, s.cleaner(target.GetCopyId()).Step(s.ctx))
	require.Equal(t, "deleted", s.target().GetState())
	_, err := hot.Open(s.ctx, target.GetBlobId())
	require.ErrorIs(t, err, model.ErrUpstreamNotFound)
	s.get()
}

func TestStandTargetUnavailable(t *testing.T) {
	for _, transferred := range []int64{0, 37} {
		t.Run(fmt.Sprintf("%d_bytes", transferred), func(t *testing.T) {
			s := newStand(t)
			hot := s.storages[model.StorageClassHot]
			attempts := 0
			s.storages[model.StorageClassHot] = &storageFault{StorageClient: hot, store: func(ctx context.Context, id string, size int64, body io.Reader) (string, error) {
				attempts++
				_, err := io.CopyN(io.Discard, body, transferred)
				if err != nil {
					return "", err
				}
				return "", model.ErrUpstreamUnavailable
			}}
			s.submit()
			s.step()
			s.outcome(model.ActionFailed, model.ReasonUpstreamUnavailable)
			require.Equal(t, s.cfg.MaxAttempts, attempts)
			require.Equal(t, transferred*int64(attempts), s.action().BytesCharged)
			require.Equal(t, "cleanup_pending", s.target().GetState())
			s.get()
		})
	}
}

func TestStandNoCapacity(t *testing.T) {
	s := newStand(t)
	s.placements = &placementFault{PlacementClient: s.placements, reserve: func(context.Context, model.ActionSpec) (model.PlacementCopy, error) {
		return model.PlacementCopy{}, model.ErrNoCapacity
	}}
	dependent := s.spec
	dependent.ActionID = uuid.NewString()
	dependent.DependsOn = []string{s.spec.ActionID}
	third := dependent
	third.ActionID = uuid.NewString()
	third.DependsOn = []string{dependent.ActionID}
	s.submit(s.spec, dependent, third)
	s.step()
	s.outcome(model.ActionFailed, model.ReasonNoCapacity)
	require.Zero(t, s.action().BytesCharged)
	require.Empty(t, s.copies())
	for _, id := range []string{dependent.ActionID, third.ActionID} {
		a, err := s.actions.Get(s.ctx, id)
		require.NoError(t, err)
		require.Equal(t, model.ActionCancelled, a.Status)
		require.Equal(t, model.ReasonDependencyFailed, a.Reason)
	}
}

func TestStandOverwriteDuringCopy(t *testing.T) {
	s := newStand(t)
	original := s.placement()
	hot := s.storages[model.StorageClassHot]
	replacement := []byte("new object version")
	s.storages[model.StorageClassHot] = &storageFault{StorageClient: hot, store: func(ctx context.Context, id string, size int64, body io.Reader) (string, error) {
		s.put(replacement)
		return hot.Store(ctx, id, size, body)
	}}
	s.submit()
	s.step()
	s.outcome(model.ActionFailed, model.ReasonStale)
	current := s.placement()
	require.NotEqual(t, original.VersionID, current.VersionID)
	require.Equal(t, int64(1), current.Generation)
	s.body = replacement
	s.get()
	require.Equal(t, "cleanup_pending", s.target().GetState())
}

func TestStandDeleteDuringCopy(t *testing.T) {
	s := newStand(t)
	source := s.placement()
	before, err := s.placementAPI.ListStorageClasses(s.ctx, &metadatav1.ListStorageClassesRequest{})
	require.NoError(t, err)
	hot := s.storages[model.StorageClassHot]
	s.storages[model.StorageClassHot] = &storageFault{StorageClient: hot, store: func(ctx context.Context, id string, size int64, body io.Reader) (string, error) {
		resp := s.request(http.MethodDelete, nil)
		resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		return hot.Store(ctx, id, size, body)
	}}
	s.submit()
	s.step()
	s.outcome(model.ActionFailed, model.ReasonStale)
	require.Equal(t, "cleanup_pending", s.target().GetState())
	after, err := s.placementAPI.ListStorageClasses(s.ctx, &metadatav1.ListStorageClassesRequest{})
	require.NoError(t, err)
	require.Equal(t, classUsed(before, "COLD"), classUsed(after, "COLD"), "DELETE must retain the source class charge")
	for _, c := range after.GetStorageClasses() {
		if c.GetName() == "COLD" {
			for _, old := range before.GetStorageClasses() {
				if old.GetName() == "COLD" {
					require.Equal(t, old.GetPlacedBytes()-source.SizeBytes, c.GetPlacedBytes())
					require.Equal(t, old.GetCopyBytes()+source.SizeBytes, c.GetCopyBytes())
				}
			}
		}
	}
	resp := s.request(http.MethodGet, nil)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	// The stand's Metadata keeps orphan blobs for its default ten minutes.
	reader, err := s.storages[source.Class].Open(s.ctx, source.BlobID)
	require.NoError(t, err)
	reader.Close()
	due, err := s.placementAPI.ListDueCleanups(s.ctx, &metadatav1.ListDueCleanupsRequest{Limit: 1000})
	require.NoError(t, err)
	for _, c := range due.GetCopies() {
		require.NotEqual(t, source.BlobID, c.GetBlobId(), "orphan must not be due before the configured delay")
	}
}

func TestStandConcurrentReads(t *testing.T) {
	s := newStand(t)
	real := s.placements
	ready, switched := make(chan struct{}), make(chan struct{})
	s.placements = &placementFault{PlacementClient: real, switchPlacement: func(ctx context.Context, req model.SwitchRequest) error {
		close(ready)
		select {
		case <-switched:
		case <-ctx.Done():
			return ctx.Err()
		}
		return real.SwitchPlacement(ctx, req)
	}}
	s.submit()
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.executor().Run(ctx) }()
	select {
	case <-ready:
	case <-s.ctx.Done():
		t.Fatal(s.ctx.Err())
	}
	readCtx, stopReads := context.WithCancel(s.ctx)
	defer stopReads()
	readDone := make(chan error, 1)
	var reads atomic.Int64
	go func() {
		for readCtx.Err() == nil {
			req, err := http.NewRequestWithContext(readCtx, http.MethodGet, s.gateway+"/objects/"+s.bucket+"/"+s.key, nil)
			if err != nil {
				readDone <- err
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				if readCtx.Err() != nil {
					readDone <- nil
				} else {
					readDone <- err
				}
				return
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readCtx.Err() != nil {
				readDone <- nil
				return
			}
			if err != nil {
				readDone <- err
				return
			}
			if resp.StatusCode != http.StatusOK || !bytes.Equal(body, s.body) {
				readDone <- fmt.Errorf("concurrent GET: status=%d or incorrect bytes", resp.StatusCode)
				return
			}
			reads.Add(1)
		}
		readDone <- nil
	}()
	require.Eventually(t, func() bool { return reads.Load() >= 3 }, 3*time.Second, time.Millisecond)
	// Leave a source response open across the switch, then compare the whole body.
	response := s.request(http.MethodGet, nil)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	prefix := make([]byte, 17)
	_, err := io.ReadFull(response.Body, prefix)
	require.NoError(t, err)
	close(switched)
	require.Eventually(t, func() bool { return s.action().Status == model.ActionSucceeded }, 5*time.Second, 5*time.Millisecond)
	afterSwitch := reads.Load()
	require.Eventually(t, func() bool { return reads.Load() >= afterSwitch+3 }, 3*time.Second, time.Millisecond)
	stopReads()
	require.NoError(t, <-readDone)
	rest, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, s.body, append(prefix, rest...))
	cancel()
	require.NoError(t, <-done)
	s.outcome(model.ActionSucceeded, model.ReasonNone)
}

func TestStandCleanupFailureAndRetry(t *testing.T) {
	s := newStand(t)
	// This object has no concurrent readers. Only this executor's source delay is shortened;
	// Metadata's orphan delay and the running Gateway configuration stay unchanged.
	s.cfg.SourceCleanupDelay = 50 * time.Millisecond
	s.submit()
	s.step()
	s.outcome(model.ActionSucceeded, model.ReasonNone)
	var source *metadatav1.PlacementCopyInfo
	for _, c := range s.copies() {
		if c.GetRole() == "source" {
			source = c
		}
	}
	require.NotNil(t, source)
	before, err := s.placementAPI.ListStorageClasses(s.ctx, &metadatav1.ListStorageClassesRequest{})
	require.NoError(t, err)
	cold := s.storages[model.StorageClassCold]
	calls := 0
	s.storages[model.StorageClassCold] = &storageFault{StorageClient: cold, delete: func(ctx context.Context, id string) error {
		calls++
		if calls == 1 {
			return errors.New("injected delete failure")
		}
		return cold.Delete(ctx, id)
	}}
	cleaner := s.cleaner(source.GetCopyId())
	require.Eventually(t, func() bool {
		copies, err := s.placements.ListDueCleanups(s.ctx, 1000)
		require.NoError(t, err)
		for _, c := range copies {
			if c.CopyID == source.GetCopyId() {
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
	require.ErrorContains(t, cleaner.Step(s.ctx), "injected delete failure")
	for _, c := range s.copies() {
		if c.GetCopyId() == source.GetCopyId() {
			require.Equal(t, "cleanup_failed", c.GetState())
		}
	}
	failed, err := s.placementAPI.ListStorageClasses(s.ctx, &metadatav1.ListStorageClassesRequest{})
	require.NoError(t, err)
	require.Equal(t, classUsed(before, "COLD"), classUsed(failed, "COLD"))
	require.NoError(t, s.api.SetPaused(s.ctx, true))
	require.NoError(t, cleaner.Step(s.ctx))
	after, err := s.placementAPI.ListStorageClasses(s.ctx, &metadatav1.ListStorageClassesRequest{})
	require.NoError(t, err)
	require.Equal(t, classUsed(before, "COLD")-s.spec.SizeBytes, classUsed(after, "COLD"))
	_, err = cold.Open(s.ctx, source.GetBlobId())
	require.ErrorIs(t, err, model.ErrUpstreamNotFound)
	for _, c := range s.copies() {
		if c.GetCopyId() == source.GetCopyId() {
			require.Equal(t, "deleted", c.GetState())
		}
	}
}
func classUsed(resp *metadatav1.ListStorageClassesResponse, class string) int64 {
	for _, c := range resp.GetStorageClasses() {
		if c.GetName() == class {
			return c.GetPlacedBytes() + c.GetCopyBytes()
		}
	}
	return -1
}

func TestStandPinnedAfterSubmit(t *testing.T) {
	s := newStand(t)
	s.submit()
	_, err := s.api.AddPin(s.ctx, model.Pin{Bucket: s.bucket, Key: s.key, Reason: "stand", Author: "test"})
	require.NoError(t, err)
	s.step()
	s.outcome(model.ActionCancelled, model.ReasonPinned)
	require.Zero(t, s.action().BytesCharged)
	require.Empty(t, s.copies())
}
func TestStandMinimumInterval(t *testing.T) {
	s := newStand(t)
	s.submit()
	s.step()
	require.NoError(t, s.api.SetMinInterval(s.ctx, time.Hour))
	p := s.placement()
	s.spec.ActionID = uuid.NewString()
	s.spec.ExpectedGeneration = p.Generation
	s.spec.SourceClass = p.Class
	s.spec.TargetClass = model.StorageClassCold
	s.submit()
	s.step()
	s.outcome(model.ActionCancelled, model.ReasonMinInterval)
	require.Zero(t, s.action().BytesCharged)
}
func TestStandBudgetWindow(t *testing.T) {
	s := newStand(t)
	require.NoError(t, s.api.SetBudget(s.ctx, s.spec.SizeBytes, time.Second))
	s.submit()
	s.step()
	s.outcome(model.ActionSucceeded, model.ReasonNone)
	p := s.placement()
	s.spec.ActionID = uuid.NewString()
	s.spec.ExpectedGeneration = p.Generation
	s.spec.SourceClass = p.Class
	s.spec.TargetClass = model.StorageClassCold
	s.submit()
	details, err := s.api.GetAction(s.ctx, s.spec.ActionID)
	require.NoError(t, err)
	require.Equal(t, model.BlockedByBudget, details.BlockedBy)
	worked, err := s.executor().Step(s.ctx)
	require.NoError(t, err)
	require.False(t, worked)
	e := s.executor()
	require.Eventually(t, func() bool { worked, err := e.Step(s.ctx); require.NoError(t, err); return worked }, 4*time.Second, 10*time.Millisecond)
	s.outcome(model.ActionSucceeded, model.ReasonNone)
	s.get()
}
