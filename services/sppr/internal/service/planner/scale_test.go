package planner

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/stretchr/testify/require"
)

// TestPlanningScale checks the thesis acceptance profile: 10 000 objects of
// 1 MiB, a day of history at 20 reads/s, a full plan within 30 s and 512 MiB.
// It measures features, forecasts and the plan from minute aggregates already
// in memory, i.e. what one cycle does after reading PostgreSQL and Metadata.
// Run with SPPR_PLANNER_SCALE=1; it is skipped otherwise because it allocates
// hundreds of megabytes.
func TestPlanningScale(t *testing.T) {
	if os.Getenv("SPPR_PLANNER_SCALE") == "" {
		t.Skip("set SPPR_PLANNER_SCALE=1 to run the acceptance-profile measurement")
	}
	const (
		objects    = 10_000
		readsPerS  = 20
		hotObjects = 1_000
	)
	at := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	rng := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rng, 1.1, 1, objects-1)
	catalog := make([]model.Placement, objects)
	versions := make([]model.Version, objects)
	for i := range catalog {
		class := model.StorageClassCold
		if i%(objects/hotObjects) == 0 {
			class = model.StorageClassHot
		}
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		catalog[i] = model.Placement{VersionID: id, Bucket: "bench", Key: fmt.Sprintf("k%05d", i), IsCurrent: true, Class: class,
			Generation: 1, SizeBytes: model.MiB, CreatedAt: at.Add(-48 * time.Hour), UpdatedAt: at.Add(-48 * time.Hour)}
		versions[i] = model.Version{VersionRef: model.VersionRef{Bucket: "bench", ObjectKey: catalog[i].Key, VersionID: id},
			SizeBytes: model.MiB, CreatedAt: catalog[i].CreatedAt}
	}
	var reads []model.MinuteReads
	perMinute := make(map[int]int64)
	for m := 1; m <= 24*60; m++ {
		clear(perMinute)
		for i := 0; i < readsPerS*60; i++ {
			perMinute[int(zipf.Uint64())]++
		}
		for obj, n := range perMinute {
			reads = append(reads, model.MinuteReads{VersionRef: versions[obj].VersionRef,
				Minute: at.Add(-time.Duration(m) * time.Minute), Reads: n, Bytes: n * model.MiB})
		}
	}
	t.Logf("history: %d minute aggregate rows for %d reads", len(reads), readsPerS*86400)

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()

	start := time.Now()
	coverage := model.Coverage{Known: true, Start: at.Add(-72 * time.Hour), Watermark: at}
	rows, err := features.NewService().Build(context.Background(), model.FeatureRequest{At: at, Catalog: versions, Reads: reads, Coverage: coverage})
	require.NoError(t, err)
	predictor := ewma.NewPredictor()
	in := model.PlanInput{
		At: at, MethodVersion: predictor.MethodVersion(), BudgetBytes: 256 * model.MiB,
		Params: model.PlannerParams{Horizon: time.Hour, HotFixedMs: 1, HotMiBPerMs: 0.5, ColdFixedMs: 20, ColdMiBPerMs: 0.05,
			PenaltyMsPerMiB: 2, MinGainMs: 100, HotUtilization: 0.9, ColdUtilization: 0.9, MaxRejectedSaved: 1000},
		Hot:  model.ClassState{CapacityBytes: hotObjects * model.MiB, MaxUtilization: 1, UsedBytes: hotObjects * model.MiB},
		Cold: model.ClassState{CapacityBytes: 2 * objects * model.MiB, MaxUtilization: 1, UsedBytes: (objects - hotObjects) * model.MiB},
	}
	byVersion := make(map[string]model.FeatureSnapshot, len(rows))
	for _, r := range rows {
		byVersion[r.VersionID] = r
	}
	for _, p := range catalog {
		it := model.PlanItem{Placement: p, ReadFraction: 1}
		if fs, ok := byVersion[p.VersionID]; ok {
			f := fs.Features
			it.Features = &f
			if forecast, err := predictor.Predict(fs, in.Params.Horizon); err == nil {
				it.Forecast = &forecast
			}
		}
		in.Items = append(in.Items, it)
	}
	n := 0
	res, err := Build(in, func() string { n++; return fmt.Sprintf("r%d", n) })
	require.NoError(t, err)
	elapsed := time.Since(start)
	close(stop)
	<-sampled

	used := float64(peak.Load()-min(peak.Load(), base.HeapAlloc)) / model.MiB
	total := float64(peak.Load()) / model.MiB
	t.Logf("plan: %d actions, %d MiB copied, net gain %.0f ms; %d rejected (%d kept)",
		res.Summary.Actions, res.Summary.CopyBytes/model.MiB, res.Summary.NetGainMs, res.Summary.Rejected, res.Summary.RejectedKept)
	t.Logf("took %s; peak heap %.0f MiB, of which %.0f MiB above the loaded history", elapsed, total, used)
	require.Less(t, elapsed, 30*time.Second)
	require.Less(t, total, 512.0)
	require.Positive(t, res.Summary.Actions)
}
