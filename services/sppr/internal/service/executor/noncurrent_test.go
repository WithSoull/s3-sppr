package executor_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

// An overwritten version may only leave HOT; Metadata enforces the same rule.
func TestProcessNoncurrentVersion(t *testing.T) {
	cases := []struct {
		name   string
		source model.StorageClass
		target model.StorageClass
		build  func(*deps)
		want   []step
	}{
		{
			name:   "evicted from HOT to COLD",
			source: model.StorageClassHot, target: model.StorageClassCold,
			build: func(d *deps) {
				d.hot.OpenMock.Return(io.NopCloser(bytes.NewReader(content)), nil)
				d.cold.StoreMock.Set(func(_ context.Context, _ string, _ int64, body io.Reader) (string, error) {
					got, _ := io.ReadAll(body)
					return md5hex(got), nil
				})
				switches(d, nil)
			},
			want: []step{copying(), switching(), succeeded()},
		},
		{
			name:   "promotion to HOT is stale before copying",
			source: model.StorageClassCold, target: model.StorageClassHot,
			build: func(d *deps) { aborts(d, model.ReasonStale, nil) },
			want:  []step{aborting(model.ReasonStale, 0), failed(model.ReasonStale)},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			a := running(model.StepReserving, model.ReasonNone, true)
			a.SourceClass, a.TargetClass = tt.source, tt.target
			d := newDeps(mc, a)
			fresh(d)
			p := placement(1, time.Now())
			p.IsCurrent, p.Class = false, tt.source
			d.placements.GetPlacementMock.Return(p, nil)
			d.checks(false, time.Hour)
			target := reserved()
			target.Class, target.ExpectedClass = tt.target, tt.source
			d.placements.ReserveCopyMock.Return(target, nil)
			tt.build(d)
			worked, err := d.executor().Step(context.Background())
			require.True(t, worked)
			require.NoError(t, err)
			require.Equal(t, tt.want, d.recorded)
		})
	}
}
