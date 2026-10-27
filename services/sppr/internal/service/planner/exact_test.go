package planner

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// exact enumerates every placement of the versions and returns the largest
// change of G(h) that satisfies HOT capacity and the copy budget. COLD is
// assumed large enough, as in the generated inputs.
func exact(in model.PlanInput) float64 {
	cands := make([]*candidate, len(in.Items))
	s := &planState{params: in.Params}
	for i := range in.Items {
		cands[i] = s.evaluate(&in.Items[i])
	}
	hotUsable := usable(in.Hot, in.Params.HotUtilization)
	var hotOther int64 = in.Hot.UsedBytes
	for _, c := range cands {
		if c.item.Class == model.StorageClassHot {
			hotOther -= c.item.SizeBytes
		}
	}
	best := 0.0
	for mask := 0; mask < 1<<len(cands); mask++ {
		hot, moved := hotOther, int64(0)
		var delta float64
		for i, c := range cands {
			inHot := mask&(1<<i) != 0
			if inHot {
				hot += c.item.SizeBytes
			}
			wasHot := c.item.Class == model.StorageClassHot
			if inHot != wasHot {
				moved += c.item.SizeBytes
				if inHot {
					delta += c.gain
				} else {
					delta -= c.gain
				}
			}
		}
		if hot > hotUsable || moved > in.BudgetBytes {
			continue
		}
		delta -= in.Params.PenaltyMsPerMiB * float64(moved) / model.MiB
		if delta >= in.Params.MinGainMs && delta > best {
			best = delta
		}
	}
	return best
}

func randomInput(rng *rand.Rand, equalSizes bool) model.PlanInput {
	n := 3 + rng.Intn(8)
	hotSlots := int64(1 + rng.Intn(n))
	var items []model.PlanItem
	inHot := int64(0)
	for i := 0; i < n; i++ {
		class := model.StorageClassCold
		if inHot < hotSlots && rng.Intn(2) == 0 {
			class = model.StorageClassHot
			inHot++
		}
		size := int64(model.MiB)
		if !equalSizes {
			size = int64(1+rng.Intn(4)) * model.MiB / 2
		}
		items = append(items, item(fmt.Sprintf("v%02d", i), class, float64(rng.Intn(20)), sized(size)))
	}
	in := input(hotSlots, 1000, int64(1+rng.Intn(2*n)), items...)
	if !equalSizes {
		// Mixed sizes may start above HOT capacity; keep the start feasible.
		in.Hot.CapacityBytes = max(in.Hot.CapacityBytes, in.Hot.UsedBytes)
	}
	in.Params.PenaltyMsPerMiB = float64(rng.Intn(40))
	return in
}

// For equal sizes and Δmin = 0 the thesis claims the greedy plan is optimal.
func TestBuildMatchesExactForEqualSizes(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for run := 0; run < 2000; run++ {
		in := randomInput(rng, true)
		res, err := Build(in, ids())
		require.NoError(t, err)
		want := exact(in)
		if !assert.InDelta(t, want, res.Summary.NetGainMs, 1e-6, "run %d: %+v", run, in) {
			return
		}
	}
}

// With mixed sizes the greedy plan is a heuristic: it must stay feasible and
// never beat the optimum; how far it falls short is reported, not asserted.
func TestBuildWithMixedSizesIsFeasibleHeuristic(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	var runs, optimal, missed int
	var ratios float64
	for run := 0; run < 2000; run++ {
		in := randomInput(rng, false)
		res, err := Build(in, ids())
		require.NoError(t, err)
		want := exact(in)
		got := res.Summary.NetGainMs
		require.LessOrEqual(t, got, want+1e-6, "run %d", run)
		require.LessOrEqual(t, res.Summary.CopyBytes, in.BudgetBytes)
		runs++
		switch {
		case math.Abs(want-got) < 1e-6:
			optimal++
			ratios++
		case got < 1e-6:
			missed++
		default:
			ratios += got / want
		}
	}
	t.Logf("greedy optimal in %d of %d runs, found nothing in %d while a gain existed; mean share of the optimum %.3f",
		optimal, runs, missed, ratios/float64(runs))
}
