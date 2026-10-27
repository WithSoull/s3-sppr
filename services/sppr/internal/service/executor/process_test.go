package executor_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

// TestProcessNewAction covers each outcome of steps 2-6 for a freshly claimed action.
func TestProcessNewAction(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name    string
		build   func(*deps)
		want    []step
		wantErr bool
	}{
		{
			name: "copies COLD to HOT and switches",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Expect(minimock.AnyContext, versionID).Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.sink(nil)
				switches(d, nil)
			},
			want: []step{copying(), switching(), succeeded()},
		},
		{
			name: "pinned key is cancelled and refunded",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(true, time.Hour)
			},
			want: []step{refunded(model.ActionCancelled, model.ReasonPinned)},
		},
		{
			name: "version moved within the minimum interval is cancelled",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(2, now.Add(-time.Minute)), nil)
				d.checks(false, time.Hour)
			},
			want: []step{refunded(model.ActionCancelled, model.ReasonMinInterval)},
		},
		{
			name: "version moved before the minimum interval proceeds",
			build: func(d *deps) {
				fresh(d)
				p := placement(2, now.Add(-2*time.Hour))
				d.placements.GetPlacementMock.Return(p, nil)
				d.checks(false, time.Hour)
				target := reserved()
				target.ExpectedGeneration = 2
				d.placements.ReserveCopyMock.Return(target, nil)
				d.source(content)
				d.sink(nil)
				switches(d, nil)
			},
			want: []step{copying(), switching(), succeeded()},
		},
		{
			name: "deleted version fails as stale",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(model.Placement{}, model.ErrUpstreamNotFound)
			},
			want: []step{refunded(model.ActionFailed, model.ReasonStale)},
		},
		{
			name: "Metadata unavailable before reservation fails without abort",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Times(maxAttempts).Return(model.Placement{}, unavailable())
			},
			want: []step{refunded(model.ActionFailed, model.ReasonUpstreamUnavailable)},
		},
		{
			name: "reservation ABORTED fails as stale",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(model.PlacementCopy{}, model.ErrStale)
			},
			want: []step{refunded(model.ActionFailed, model.ReasonStale)},
		},
		{
			name: "reservation RESOURCE_EXHAUSTED fails as no capacity",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(model.PlacementCopy{}, model.ErrNoCapacity)
			},
			want: []step{refunded(model.ActionFailed, model.ReasonNoCapacity)},
		},
		{
			name: "reservation unavailable is aborted in case it was made",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Times(maxAttempts).Return(model.PlacementCopy{}, unavailable())
				aborts(d, model.ReasonUpstreamUnavailable, model.ErrUpstreamNotFound)
			},
			want: []step{aborting(model.ReasonUpstreamUnavailable, 0), failed(model.ReasonUpstreamUnavailable)},
		},
		{
			name: "missing source blob is aborted",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.cold.OpenMock.Return(nil, model.ErrUpstreamNotFound)
				aborts(d, model.ReasonSourceMissing, nil)
			},
			want: []step{copying(), aborting(model.ReasonSourceMissing, 0), failed(model.ReasonSourceMissing)},
		},
		{
			name: "MD5 mismatch keeps the transferred bytes charged",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.hot.StoreMock.Set(func(_ context.Context, _ string, _ int64, body io.Reader) (string, error) {
					_, err := io.Copy(io.Discard, body)
					return "00000000000000000000000000000000", err
				})
				aborts(d, model.ReasonVerifyMismatch, nil)
			},
			want: []step{copying(), aborting(model.ReasonVerifyMismatch, size), failed(model.ReasonVerifyMismatch)},
		},
		{
			name: "short source fails verification",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content[:5])
				d.sink(errors.New("rpc error: code = InvalidArgument desc = expected 11 bytes, received 5"))
				aborts(d, model.ReasonVerifyMismatch, nil)
			},
			want: []step{copying(), aborting(model.ReasonVerifyMismatch, 5), failed(model.ReasonVerifyMismatch)},
		},
		{
			name: "Storage retry charges every attempt",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.sink(unavailable(), nil)
				switches(d, nil)
			},
			want: []step{
				copying(),
				{Status: model.ActionRunning, Step: model.StepCopying, Settle: true, Bytes: size, Reopen: true},
				switching(), succeeded(),
			},
		},
		{
			name: "Storage unavailable on every attempt is aborted",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.sink(unavailable())
				aborts(d, model.ReasonUpstreamUnavailable, nil)
			},
			want: []step{
				copying(),
				{Status: model.ActionRunning, Step: model.StepCopying, Settle: true, Bytes: size, Reopen: true},
				{Status: model.ActionRunning, Step: model.StepCopying, Settle: true, Bytes: size, Reopen: true},
				aborting(model.ReasonUpstreamUnavailable, size), failed(model.ReasonUpstreamUnavailable),
			},
		},
		{
			name: "target out of space fails as no capacity",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.sink(model.ErrNoCapacity)
				aborts(d, model.ReasonNoCapacity, nil)
			},
			want: []step{copying(), aborting(model.ReasonNoCapacity, size), failed(model.ReasonNoCapacity)},
		},
		{
			name: "switch ABORTED aborts the copy as stale",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.sink(nil)
				switches(d, model.ErrStale)
				aborts(d, model.ReasonStale, nil)
			},
			want: []step{copying(), switching(), aborting(model.ReasonStale, 0), failed(model.ReasonStale)},
		},
		{
			name: "switch whose reply was lost is found by AbortCopy",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.sink(nil)
				d.placements.SwitchPlacementMock.Times(maxAttempts).Return(unavailable())
				aborts(d, model.ReasonUpstreamUnavailable, model.ErrUpstreamPrecondition)
			},
			want: []step{copying(), switching(), aborting(model.ReasonUpstreamUnavailable, 0), succeeded()},
		},
		{
			name: "abort that cannot reach Metadata stays aborting",
			build: func(d *deps) {
				fresh(d)
				d.placements.GetPlacementMock.Return(placement(1, now), nil)
				d.checks(false, time.Hour)
				d.placements.ReserveCopyMock.Return(reserved(), nil)
				d.source(content)
				d.sink(nil)
				switches(d, model.ErrStale)
				d.placements.AbortCopyMock.Times(maxAttempts).Return(unavailable())
			},
			want:    []step{copying(), switching(), aborting(model.ReasonStale, 0)},
			wantErr: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newDeps(mc, running(model.StepReserving, model.ReasonNone, true))
			tt.build(d)
			worked, err := d.executor().Step(context.Background())
			require.True(t, worked)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, d.recorded)
		})
	}
}

func TestProcessReservationSize(t *testing.T) {
	newReservation := func(d *deps) {
		fresh(d)
		d.placements.GetPlacementMock.Return(placement(1, time.Now()), nil)
		d.checks(false, 0)
		d.placements.ReserveCopyMock.Return(reserved(), nil)
	}
	resumeReservation := func(d *deps) {
		d.placements.ListMigrationCopiesMock.Return([]model.PlacementCopy{reserved()}, nil)
	}
	type args struct {
		plannedSize int64
		step        model.ActionStep
	}
	cases := []struct {
		name      string
		args      args
		build     func(*testing.T, *deps)
		want      []step
		wantError string
	}{
		{
			name: "matching size copies and switches with the original charge",
			args: args{plannedSize: size, step: model.StepReserving},
			build: func(t *testing.T, d *deps) {
				newReservation(d)
				d.cold.OpenMock.Expect(minimock.AnyContext, sourceBlobID).
					Return(io.NopCloser(bytes.NewReader(content)), nil)
				d.hot.StoreMock.Set(func(_ context.Context, blobID string, headerSize int64, body io.Reader) (string, error) {
					require.Equal(t, targetBlobID, blobID)
					require.Equal(t, size, headerSize)
					got, err := io.ReadAll(body)
					require.NoError(t, err)
					require.Equal(t, content, got)
					return contentMD5, nil
				})
				switches(d, nil)
			},
			want: []step{copying(), switching(), succeeded()},
		},
		{
			name: "understated size aborts and refunds before copying",
			args: args{plannedSize: size - 1, step: model.StepReserving},
			build: func(t *testing.T, d *deps) {
				newReservation(d)
				aborts(d, model.ReasonStale, nil)
			},
			want:      []step{aborting(model.ReasonStale, 0), failed(model.ReasonStale)},
			wantError: fmt.Sprintf("planned size_bytes %d differs from reserved copy size_bytes %d", size-1, size),
		},
		{
			name: "overstated size also aborts and refunds before copying",
			args: args{plannedSize: size + 1, step: model.StepReserving},
			build: func(t *testing.T, d *deps) {
				newReservation(d)
				aborts(d, model.ReasonStale, nil)
			},
			want:      []step{aborting(model.ReasonStale, 0), failed(model.ReasonStale)},
			wantError: fmt.Sprintf("planned size_bytes %d differs from reserved copy size_bytes %d", size+1, size),
		},
		{
			name: "resumed reservation with a mismatched size refunds without copying",
			args: args{plannedSize: size - 1, step: model.StepReserving},
			build: func(t *testing.T, d *deps) {
				resumeReservation(d)
				aborts(d, model.ReasonStale, nil)
			},
			want:      []step{aborting(model.ReasonStale, 0), failed(model.ReasonStale)},
			wantError: fmt.Sprintf("planned size_bytes %d differs from reserved copy size_bytes %d", size-1, size),
		},
		{
			name: "interrupted copy with a mismatched size retains its unknown previous charge",
			args: args{plannedSize: size - 1, step: model.StepCopying},
			build: func(t *testing.T, d *deps) {
				resumeReservation(d)
				aborts(d, model.ReasonStale, nil)
			},
			want:      []step{aborting(model.ReasonStale, size-1), failed(model.ReasonStale)},
			wantError: fmt.Sprintf("planned size_bytes %d differs from reserved copy size_bytes %d", size-1, size),
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			a := running(tt.args.step, model.ReasonNone, true)
			a.SizeBytes = tt.args.plannedSize
			d := newDeps(mc, a)
			tt.build(t, d)
			d.actions.TransitionMock.Set(func(_ context.Context, tr model.Transition) ([]string, error) {
				d.recorded = append(d.recorded, step{tr.Status, tr.Step, tr.Reason, tr.Settle, tr.Bytes, tr.Reopen})
				if tr.Step == model.StepAborting {
					require.Equal(t, tt.wantError, tr.Error)
				}
				return nil, nil
			})
			worked, err := d.executor().Step(context.Background())
			require.True(t, worked)
			require.NoError(t, err)
			require.Equal(t, tt.want, d.recorded)
			if tt.wantError != "" {
				require.Zero(t, d.cold.OpenAfterCounter())
				require.Zero(t, d.hot.StoreAfterCounter())
				require.Zero(t, d.placements.SwitchPlacementAfterCounter())
			}
		})
	}
}
