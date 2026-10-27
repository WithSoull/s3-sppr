package planner

import (
	"fmt"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// With these coefficients a fully read 1 MiB object gains 28 ms per read in HOT:
// COLD 20 ms + 10 ms transfer, HOT 1 ms + 1 ms transfer.
func testParams() model.PlannerParams {
	return model.PlannerParams{
		Horizon:    time.Hour,
		HotFixedMs: 1, HotMiBPerMs: 1,
		ColdFixedMs: 20, ColdMiBPerMs: 0.1,
		HotUtilization: 1, ColdUtilization: 1,
		MaxRejectedSaved: -1,
	}
}

type itemOpt func(*model.PlanItem)

func item(id string, class model.StorageClass, reads float64, opts ...itemOpt) model.PlanItem {
	it := model.PlanItem{
		Placement: model.Placement{
			VersionID: id, Bucket: "b", Key: "k-" + id, IsCurrent: true,
			Class: class, Generation: 1, SizeBytes: model.MiB,
		},
		Forecast:     &model.DemandForecast{ExpectedReads: reads, Quality: model.DataQuality{Usable: true, Reason: "complete"}},
		ReadFraction: 1,
	}
	for _, o := range opts {
		o(&it)
	}
	return it
}

func sized(bytes int64) itemOpt { return func(it *model.PlanItem) { it.SizeBytes = bytes } }
func noncurrent() itemOpt {
	return func(it *model.PlanItem) { it.IsCurrent = false; it.Forecast = nil }
}
func blocked(b model.Blocker) itemOpt { return func(it *model.PlanItem) { it.Blocker = b } }
func noForecast() itemOpt             { return func(it *model.PlanItem) { it.Forecast = nil } }

func input(hotSlots, coldSlots int64, budgetSlots int64, items ...model.PlanItem) model.PlanInput {
	in := model.PlanInput{
		Params:        testParams(),
		Hot:           model.ClassState{CapacityBytes: hotSlots * model.MiB, MaxUtilization: 1},
		Cold:          model.ClassState{CapacityBytes: coldSlots * model.MiB, MaxUtilization: 1},
		BudgetBytes:   budgetSlots * model.MiB,
		Items:         items,
		MethodVersion: "ewma-1h-v1",
	}
	for _, it := range items {
		if it.Class == model.StorageClassHot {
			in.Hot.UsedBytes += it.SizeBytes
		} else {
			in.Cold.UsedBytes += it.SizeBytes
		}
	}
	return in
}

func ids() func() string {
	n := 0
	return func() string { n++; return fmt.Sprintf("r%d", n) }
}

type outcome struct {
	kind   model.RecommendationKind
	reason model.RejectReason
	deps   []string // version IDs the action depends on
}

func summarize(t *testing.T, res model.PlanResult) map[string]outcome {
	t.Helper()
	byID := make(map[string]string)
	for _, r := range res.Recommendations {
		byID[r.RecommendationID] = r.VersionID
	}
	out := make(map[string]outcome)
	for _, r := range res.Recommendations {
		_, dup := out[r.VersionID]
		require.False(t, dup, "version %s recommended twice", r.VersionID)
		o := outcome{kind: r.Kind, reason: r.Reason}
		for _, d := range r.DependsOn {
			o.deps = append(o.deps, byID[d])
		}
		require.Equal(t, r.Selected, r.Reason == model.RejectNone)
		out[r.VersionID] = o
	}
	return out
}

func TestBuild(t *testing.T) {
	tests := []struct {
		name   string
		in     model.PlanInput
		tune   func(*model.PlanInput)
		want   map[string]outcome
		budget int64 // expected remaining budget in MiB after the plan
	}{
		{
			name: "free HOT takes the highest gain per byte within budget",
			in: input(3, 10, 2,
				item("a", model.StorageClassCold, 1),
				item("b", model.StorageClassCold, 5),
				item("c", model.StorageClassCold, 3)),
			want: map[string]outcome{
				"b": {kind: model.KindPromote},
				"c": {kind: model.KindPromote},
				"a": {kind: model.KindPromote, reason: model.RejectBudget},
			},
		},
		{
			name: "full HOT swaps the coldest HOT version and promotion depends on it",
			in: input(1, 10, 4,
				item("hot", model.StorageClassHot, 1),
				item("cold", model.StorageClassCold, 10)),
			want: map[string]outcome{
				"hot":  {kind: model.KindEvict},
				"cold": {kind: model.KindPromote, deps: []string{"hot"}},
			},
		},
		{
			name: "swap is rejected when the penalty eats the difference",
			in: input(1, 10, 4,
				item("hot", model.StorageClassHot, 9),
				item("cold", model.StorageClassCold, 10)),
			tune: func(in *model.PlanInput) { in.Params.PenaltyMsPerMiB = 15 },
			want: map[string]outcome{
				"cold": {kind: model.KindPromote, reason: model.RejectGainThreshold},
			},
		},
		{
			name: "threshold rejects a promotion into free space",
			in: input(2, 10, 4,
				item("cold", model.StorageClassCold, 1)),
			tune: func(in *model.PlanInput) { in.Params.MinGainMs = 29 },
			want: map[string]outcome{
				"cold": {kind: model.KindPromote, reason: model.RejectGainThreshold},
			},
		},
		{
			name: "budget ends in the middle and the rest is rejected",
			in: input(1, 10, 1,
				item("hot", model.StorageClassHot, 1),
				item("cold", model.StorageClassCold, 10)),
			want: map[string]outcome{
				"cold": {kind: model.KindPromote, reason: model.RejectBudget},
			},
		},
		{
			name: "blocked versions stay in place with their reason",
			in: input(1, 10, 10,
				item("hot", model.StorageClassHot, 0, blocked(model.BlockerPinned)),
				item("pinned", model.StorageClassCold, 9, blocked(model.BlockerPinned)),
				item("recent", model.StorageClassCold, 8, blocked(model.BlockerMinInterval)),
				item("busy", model.StorageClassCold, 7, blocked(model.BlockerInProgress)),
				item("free", model.StorageClassCold, 6)),
			want: map[string]outcome{
				"pinned": {kind: model.KindPromote, reason: model.RejectPinned},
				"recent": {kind: model.KindPromote, reason: model.RejectMinInterval},
				"busy":   {kind: model.KindPromote, reason: model.RejectInProgress},
				"free":   {kind: model.KindPromote, reason: model.RejectHotCapacity},
			},
		},
		{
			name: "overwritten HOT version is displaced first and leftover budget evicts the rest",
			in: input(2, 10, 3,
				item("old1", model.StorageClassHot, 0, noncurrent()),
				item("old2", model.StorageClassHot, 0, noncurrent()),
				item("cold", model.StorageClassCold, 4)),
			want: map[string]outcome{
				"old1": {kind: model.KindEvict},
				"cold": {kind: model.KindPromote, deps: []string{"old1"}},
				"old2": {kind: model.KindEvictNoncurrent},
			},
		},
		{
			name: "overfull HOT is rebalanced before promotions",
			in: input(2, 10, 5,
				item("h1", model.StorageClassHot, 1),
				item("h2", model.StorageClassHot, 3),
				item("h3", model.StorageClassHot, 2),
				item("cold", model.StorageClassCold, 2)),
			tune: func(in *model.PlanInput) { in.Params.HotUtilization = 0.5 },
			want: map[string]outcome{
				"h1": {kind: model.KindRebalance},
				"h3": {kind: model.KindRebalance},
				// HOT keeps h2 (3 reads); swapping it for 2 reads is a loss.
				"cold": {kind: model.KindPromote, reason: model.RejectGainThreshold},
			},
		},
		{
			name: "no room in COLD for the displaced version",
			in: input(1, 1, 4,
				item("hot", model.StorageClassHot, 1),
				item("cold", model.StorageClassCold, 10)),
			want: map[string]outcome{
				"cold": {kind: model.KindPromote, reason: model.RejectColdCapacity},
			},
		},
		{
			name: "version without a forecast is reported, not promoted",
			in: input(2, 10, 4,
				item("new", model.StorageClassCold, 0, noForecast())),
			want: map[string]outcome{
				"new": {kind: model.KindPromote, reason: model.RejectNoForecast},
			},
		},
		{
			name: "large popular object loses to two smaller ones of higher gain per byte",
			in: input(2, 10, 10,
				item("big", model.StorageClassCold, 6, sized(2*model.MiB)),
				item("s1", model.StorageClassCold, 4),
				item("s2", model.StorageClassCold, 4)),
			want: map[string]outcome{
				"s1":  {kind: model.KindPromote},
				"s2":  {kind: model.KindPromote},
				"big": {kind: model.KindPromote, reason: model.RejectHotCapacity},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			if tt.tune != nil {
				tt.tune(&in)
			}
			res, err := Build(in, ids())
			require.NoError(t, err)
			assert.Equal(t, tt.want, summarize(t, res))
		})
	}
}

// The explanation must agree with the plan: resources move by the action size
// in order, and the summary equals the sum of decisions (case C-01).
func TestBuildExplanationMatchesPlan(t *testing.T) {
	in := input(2, 10, 6,
		item("hot1", model.StorageClassHot, 1),
		item("hot2", model.StorageClassHot, 2),
		item("old", model.StorageClassHot, 0, noncurrent()),
		item("c1", model.StorageClassCold, 9),
		item("c2", model.StorageClassCold, 5),
		item("c3", model.StorageClassCold, 1))
	in.Hot.CapacityBytes = 3 * model.MiB
	in.Params.PenaltyMsPerMiB = 2

	res, err := Build(in, ids())
	require.NoError(t, err)

	hot, cold, budget := in.Hot.UsedBytes, in.Cold.UsedBytes, in.BudgetBytes
	var copied int64
	nets := make(map[string]float64)
	for _, r := range res.Recommendations {
		e := r.Explanation
		if !r.Selected {
			assert.Equal(t, r.Reason, e.Reason)
			continue
		}
		assert.Equal(t, hot, e.Resources.HotUsedBefore, r.VersionID)
		assert.Equal(t, cold, e.Resources.ColdUsedBefore, r.VersionID)
		assert.Equal(t, budget, e.Resources.BudgetBefore, r.VersionID)
		if r.TargetClass == model.StorageClassHot {
			hot += r.SizeBytes
		} else {
			hot -= r.SizeBytes
			cold += r.SizeBytes
		}
		budget -= r.SizeBytes
		copied += r.SizeBytes
		assert.Equal(t, hot, e.Resources.HotUsedAfter, r.VersionID)
		assert.Equal(t, cold, e.Resources.ColdUsedAfter, r.VersionID)
		assert.Equal(t, budget, e.Resources.BudgetAfter, r.VersionID)
		assert.Equal(t, r.DependsOn, e.DependsOn)
		assert.InDelta(t, e.Effect.SetGainMs-e.Effect.SetPenaltyMs, e.Effect.SetNetGainMs, 1e-9)
		switch r.Kind {
		case model.KindPromote:
			assert.GreaterOrEqual(t, e.Effect.SetNetGainMs, e.Effect.MinGainMs)
			assert.InDelta(t, 28*e.Forecast.ExpectedReads, e.Effect.GainMs, 1e-9)
			assert.Len(t, e.Effect.Displaced, len(r.DependsOn))
			nets[e.Effect.Set] = e.Effect.SetNetGainMs
		case model.KindEvict:
			assert.NotEmpty(t, e.Effect.Set)
		default:
			nets[r.RecommendationID] = e.Effect.SetNetGainMs
		}
	}
	var total float64
	for _, n := range nets {
		total += n
	}
	assert.Equal(t, copied, res.Summary.CopyBytes)
	assert.InDelta(t, total, res.Summary.NetGainMs, 1e-9)
	assert.LessOrEqual(t, hot, in.Hot.CapacityBytes)
	assert.GreaterOrEqual(t, budget, int64(0))
	assert.Positive(t, res.Summary.Actions)
}

func TestBuildKeepsOnlyBestRejected(t *testing.T) {
	in := input(0, 10, 0,
		item("a", model.StorageClassCold, 1),
		item("b", model.StorageClassCold, 3),
		item("c", model.StorageClassCold, 2))
	in.Params.MaxRejectedSaved = 2
	res, err := Build(in, ids())
	require.NoError(t, err)
	require.Len(t, res.Recommendations, 2)
	assert.Equal(t, "b", res.Recommendations[0].VersionID)
	assert.Equal(t, "c", res.Recommendations[1].VersionID)
	assert.Equal(t, 3, res.Summary.Rejected)
	assert.Equal(t, 2, res.Summary.RejectedKept)
}

func TestBuildRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		tune func(*model.PlanInput)
	}{
		{"zero horizon", func(in *model.PlanInput) { in.Params.Horizon = 0 }},
		{"zero speed", func(in *model.PlanInput) { in.Params.ColdMiBPerMs = 0 }},
		{"utilization above 1", func(in *model.PlanInput) { in.Params.HotUtilization = 1.5 }},
		{"negative budget", func(in *model.PlanInput) { in.BudgetBytes = -1 }},
		{"duplicate version", func(in *model.PlanInput) { in.Items = append(in.Items, in.Items[0]) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := input(1, 1, 1, item("a", model.StorageClassCold, 1))
			tt.tune(&in)
			_, err := Build(in, ids())
			assert.Error(t, err)
		})
	}
}
