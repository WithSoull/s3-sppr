package planner

import (
	"math/rand"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func TestUniformOptimizationMatchesGeneralPath(t *testing.T) {
	rng := rand.New(rand.NewSource(210))
	for run := 0; run < 3000; run++ {
		in := randomInput(rng, true)
		// Include infeasible initial HOT, fractional unused capacity, noncurrent
		// versions and blockers: these must retain the general-path semantics.
		in.Hot.CapacityBytes += int64(rng.Intn(model.MiB))
		if run%4 == 0 {
			in.Hot.CapacityBytes /= 2
		}
		if run%3 == 0 {
			in.Items[0].IsCurrent = false
			in.Items[0].Forecast = nil
		}
		if run%5 == 0 {
			in.Items[len(in.Items)-1].Blocker = model.BlockerMinInterval
		}
		fast, err := Build(in, ids())
		require.NoError(t, err)
		// A noncurrent COLD zero-byte sentinel is neither a candidate nor part of
		// forecast percentiles. It only disables the equal-size optimization, so
		// this exercises the original general algorithm without a test-only switch.
		general := in
		general.Items = append(append([]model.PlanItem(nil), in.Items...), item("sentinel", model.StorageClassCold, 0, noncurrent(), sized(0)))
		want, err := Build(general, ids())
		require.NoError(t, err)
		require.Equal(t, want, fast, "run %d", run)
	}
}
