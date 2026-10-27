//go:build integration

package executor_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func TestStandRestart(t *testing.T) {
	cases := []struct {
		name      string
		attempts  int64
		largeBody bool
		install   func(*stand, func(context.Context) error)
	}{
		{name: "reserving", attempts: 1, install: func(s *stand, stop func(context.Context) error) {
			real := s.placements
			s.placements = &placementFault{PlacementClient: real, list: func(ctx context.Context, id string) ([]model.PlacementCopy, error) { return nil, stop(ctx) }}
		}},
		{name: "reserved_before_copying", attempts: 1, install: func(s *stand, stop func(context.Context) error) {
			real := s.placements
			s.placements = &placementFault{PlacementClient: real, reserve: func(ctx context.Context, a model.ActionSpec) (model.PlacementCopy, error) {
				c, err := real.ReserveCopy(ctx, a)
				if err != nil {
					return c, err
				}
				return c, stop(ctx)
			}}
		}},
		{name: "copying", attempts: 2, largeBody: true, install: func(s *stand, stop func(context.Context) error) {
			hot := s.storages[model.StorageClassHot]
			s.storages[model.StorageClassHot] = &storageFault{StorageClient: hot, store: func(ctx context.Context, id string, size int64, body io.Reader) (string, error) {
				return hot.Store(ctx, id, size, &interruptedReader{Reader: body, remaining: 1 << 20, stop: func() error { return stop(ctx) }})
			}}
		}},
		{name: "switching", attempts: 2, install: func(s *stand, stop func(context.Context) error) {
			real := s.placements
			s.placements = &placementFault{PlacementClient: real, switchPlacement: func(ctx context.Context, r model.SwitchRequest) error { return stop(ctx) }}
		}},
		{name: "after_switch", attempts: 1, install: func(s *stand, stop func(context.Context) error) {
			real := s.placements
			s.placements = &placementFault{PlacementClient: real, switchPlacement: func(ctx context.Context, r model.SwitchRequest) error {
				if err := real.SwitchPlacement(ctx, r); err != nil {
					return err
				}
				return stop(ctx)
			}}
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s := newStand(t)
			if tt.largeBody {
				s.body = bytes.Repeat([]byte("0123456789abcdef"), 131073)
				s.put(s.body)
				p := s.placement()
				s.spec.VersionID = p.VersionID
				s.spec.SizeBytes = p.SizeBytes
			}
			realPlacement := s.placements
			realHot := s.storages[model.StorageClassHot]
			reached := make(chan struct{})
			tt.install(s, func(ctx context.Context) error { close(reached); <-ctx.Done(); return ctx.Err() })
			s.submit()
			ctx, cancel := context.WithCancel(s.ctx)
			defer cancel()
			old := s.executor()
			done := make(chan error, 1)
			go func() { done <- old.Run(ctx) }()
			select {
			case <-reached:
			case <-s.ctx.Done():
				t.Fatal(s.ctx.Err())
			}
			require.Equal(t, model.ActionRunning, s.action().Status)
			before := s.copies()
			cancel()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("old executor did not stop")
			}
			s.placements = realPlacement
			s.storages[model.StorageClassHot] = realHot
			// A new instance shares only durable dependencies, never the old held map.
			restarted := s.executor()
			worked, err := restarted.Step(s.ctx)
			require.NoError(t, err)
			require.True(t, worked)
			s.outcome(model.ActionSucceeded, model.ReasonNone)
			require.Equal(t, tt.attempts*s.spec.SizeBytes, s.action().BytesCharged)
			require.Equal(t, int32(tt.attempts), s.action().Attempts)
			p := s.placement()
			require.Equal(t, int64(2), p.Generation)
			require.Equal(t, model.StorageClassHot, p.Class)
			copies := s.copies()
			targets, sources := 0, 0
			for _, c := range copies {
				if c.GetRole() == "target" {
					targets++
					require.Equal(t, "promoted", c.GetState())
					require.Equal(t, p.BlobID, c.GetBlobId())
				}
				if c.GetRole() == "source" {
					sources++
				}
			}
			require.Equal(t, 1, targets)
			require.Equal(t, 1, sources)
			for _, c := range before {
				if c.GetRole() == "target" {
					require.Equal(t, c.GetBlobId(), p.BlobID)
				}
			}
			events, err := s.actions.ListEvents(s.ctx, s.spec.ActionID)
			require.NoError(t, err)
			successes := 0
			for _, e := range events {
				if e.ToStatus == model.ActionSucceeded {
					successes++
				}
			}
			require.Equal(t, 1, successes)
			s.get()
		})
	}
}
