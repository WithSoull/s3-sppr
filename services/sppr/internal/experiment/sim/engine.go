package sim

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/planner"
)

type Read struct {
	At     int64
	Object int
	Bytes  int64
}
type Frame struct {
	At            int64
	Labels        []int64
	Features      []model.Features
	Forecasts     map[string][]float64
	ModelVersions map[string]string
}
type Latency struct {
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	Reads  int64   `json:"reads"`
	Errors int64   `json:"errors"`
}
type Result struct {
	Config      Config            `json:"config"`
	Scenario    string            `json:"scenario"`
	Seed        uint64            `json:"seed"`
	Policy      string            `json:"policy"`
	Budget      int64             `json:"budget_bytes"`
	Versions    map[string]string `json:"versions"`
	Latency     Latency           `json:"latency"`
	HotShare    float64           `json:"hot_read_share"`
	CopyBytes   int64             `json:"copy_bytes"`
	Migrations  int64             `json:"migrations"`
	Violations  int               `json:"violations"`
	PlanSeconds float64           `json:"plan_seconds"`
	Models      []Training        `json:"model_updates,omitempty"`
	ForecastMAE float64           `json:"forecast_mae"`
	raw         []int64
}
type action struct {
	rec    model.Recommendation
	object int
	ready  int64
}
type completion struct {
	at     int64
	order  int
	action *action
	read   *Read
}
type completions []completion

func (h completions) Len() int { return len(h) }
func (h completions) Less(i, j int) bool {
	if h[i].at == h[j].at {
		return h[i].order < h[j].order
	}
	return h[i].at < h[j].at
}
func (h completions) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *completions) Push(x any)   { *h = append(*h, x.(completion)) }
func (h *completions) Pop() any     { a := *h; x := a[len(a)-1]; *h = a[:len(a)-1]; return x }

type Engine struct {
	cfg           Config
	catalog       []model.Version
	placement     []model.Placement
	index         map[string]int
	reads         []Read
	arrived, head int
	now           int64
	running       int
	jobs          completions
	order         int
	pending       []action
	busy          []bool
	done          map[string]int64
	hot, cold     int64
	total, copies int64
	result        Result
	sum           float64
	hotReads      int64
	histogram     map[int64]int64
	mae           float64
	predictions   int64
}

func NewEngine(c Config, catalog []model.Version, reads []Read, policy string, budget int64) (*Engine, error) {
	e := &Engine{cfg: c, catalog: catalog, reads: reads, index: map[string]int{}, busy: make([]bool, len(catalog)), done: map[string]int64{}, histogram: map[int64]int64{}, result: Result{Config: c, Scenario: c.Generator.Scenario, Seed: c.Generator.Seed, Policy: policy, Budget: budget, Versions: map[string]string{"simulator": Version, "features": model.FeatureRulesVersion, "generator": "synthetic-v1", "method": methodVersion(policy)}}}
	for i, v := range catalog {
		if _, ok := e.index[v.VersionID]; ok {
			return nil, fmt.Errorf("duplicate version")
		}
		e.index[v.VersionID] = i
		e.placement = append(e.placement, model.Placement{VersionID: v.VersionID, Bucket: v.Bucket, Key: v.ObjectKey, SizeBytes: v.SizeBytes, CreatedAt: v.CreatedAt, UpdatedAt: v.CreatedAt, Generation: 1, IsCurrent: true, Class: model.StorageClassCold})
		e.cold += v.SizeBytes
	}
	for i, r := range reads {
		if r.Object < 0 || r.Object >= len(catalog) || r.Bytes < 0 || r.Bytes > catalog[r.Object].SizeBytes || i > 0 && r.At < reads[i-1].At {
			return nil, fmt.Errorf("invalid read stream")
		}
	}
	e.total = e.cold
	return e, e.check()
}
func (e *Engine) check() error {
	if e.hot+e.cold != e.total+e.copies || e.copies < 0 || e.hot < 0 || e.cold < 0 || e.hot > int64(float64(e.cfg.HotBytes)*e.cfg.Utilization) || e.cold > int64(float64(e.cfg.ColdBytes)*e.cfg.Utilization) || len(e.jobs) > e.cfg.Slots || e.running > e.cfg.MigrationParallelism {
		return fmt.Errorf("sim invariant: occupancy HOT=%d COLD=%d, slots=%d migrations=%d", e.hot, e.cold, len(e.jobs), e.running)
	}
	return nil
}
func (e *Engine) Plan(f Frame) error {
	if f.At < e.now {
		return fmt.Errorf("non-monotone planning time")
	}
	if len(f.Features) != len(e.placement) || len(f.Forecasts[e.result.Policy]) != len(e.placement) {
		return fmt.Errorf("cache frame shape mismatch")
	}
	if err := e.Advance(f.At); err != nil {
		return err
	}
	at := e.cfg.Generator.Start.Add(time.Duration(f.At))
	in := model.PlanInput{At: at, Params: e.cfg.params(), Hot: model.ClassState{CapacityBytes: e.cfg.HotBytes, UsedBytes: e.hot}, Cold: model.ClassState{CapacityBytes: e.cfg.ColdBytes, UsedBytes: e.cold}, BudgetBytes: e.result.Budget, MethodVersion: methodVersion(e.result.Policy)}
	// Pending target reservations are included just as in Planner.snapshot.
	for _, a := range e.pending {
		if a.rec.TargetClass == model.StorageClassHot {
			in.Hot.UsedBytes += a.rec.SizeBytes
		} else {
			in.Cold.UsedBytes += a.rec.SizeBytes
		}
	}
	for i, p := range e.placement {
		ft := &f.Features[i]
		fraction := 1.0
		if ft.Reads24h > 0 {
			fraction = ft.MeanReadFraction
		}
		it := model.PlanItem{Placement: p, Features: ft, ReadFraction: fraction, Forecast: &model.DemandForecast{ExpectedReads: f.Forecasts[e.result.Policy][i]}}
		if !finite(it.Forecast.ExpectedReads) || it.Forecast.ExpectedReads < 0 {
			return fmt.Errorf("invalid cached forecast")
		}
		if e.busy[i] {
			it.Blocker = model.BlockerInProgress
		} else if p.Generation > 1 && at.Sub(p.UpdatedAt) < time.Duration(e.cfg.MinIntervalSeconds)*time.Second {
			it.Blocker = model.BlockerMinInterval
		}
		in.Items = append(in.Items, it)
	}
	started := time.Now()
	plan, err := planner.Build(in, func() string { e.order++; return fmt.Sprintf("r%d", e.order) })
	e.result.PlanSeconds += time.Since(started).Seconds()
	if err != nil {
		return err
	}
	if plan.Summary.CopyBytes > in.BudgetBytes {
		return fmt.Errorf("sim invariant: cycle budget exceeded")
	}
	var charged int64
	for _, r := range plan.Recommendations {
		if !r.Selected {
			continue
		}
		charged += r.SizeBytes
		if charged > e.result.Budget {
			return fmt.Errorf("sim invariant: action budget exceeded")
		}
		i := e.index[r.VersionID]
		if e.busy[i] {
			return fmt.Errorf("sim invariant: duplicate action")
		}
		e.busy[i] = true
		e.pending = append(e.pending, action{r, i, f.At + int64(e.cfg.ConfirmSeconds)*int64(time.Second)})
	}
	if charged != plan.Summary.CopyBytes {
		return fmt.Errorf("sim invariant: budget summary differs from actions")
	}
	e.result.Versions["model"] = f.ModelVersions[e.result.Policy]
	return e.check()
}

// Advance processes completions before admissions; equal-time reads precede
// copies. A ready copy joins the same FIFO once its dependencies have finished.
func (e *Engine) Advance(until int64) error {
	for {
		next := int64(math.MaxInt64)
		if e.arrived < len(e.reads) {
			next = e.reads[e.arrived].At
		}
		if len(e.jobs) > 0 {
			next = min(next, e.jobs[0].at)
		}
		if ready, ok := e.copyReady(); ok && e.running < e.cfg.MigrationParallelism && len(e.jobs) < e.cfg.Slots {
			next = min(next, max(e.now, ready))
		}
		if next >= until {
			e.now = until
			return nil
		}
		if next == math.MaxInt64 {
			return fmt.Errorf("sim stalled")
		}
		e.now = next
		for len(e.jobs) > 0 && e.jobs[0].at <= e.now {
			j := heap.Pop(&e.jobs).(completion)
			if j.action != nil {
				a := j.action
				p := &e.placement[a.object]
				if p.Class != a.rec.SourceClass || p.Generation != a.rec.ExpectedGeneration {
					return fmt.Errorf("sim invariant: lost source version")
				}
				if p.Class == model.StorageClassHot {
					e.hot -= p.SizeBytes
				} else {
					e.cold -= p.SizeBytes
				}
				p.Class = a.rec.TargetClass
				p.Generation++
				p.UpdatedAt = e.cfg.Generator.Start.Add(time.Duration(j.at))
				e.busy[a.object] = false
				e.done[a.rec.RecommendationID] = j.at
				e.running--
				e.copies -= p.SizeBytes
				e.result.CopyBytes += p.SizeBytes
				e.result.Migrations++
			} else {
				d := j.at - j.read.At
				if e.cfg.RawLatencies {
					e.result.raw = append(e.result.raw, d)
				}
				e.histogram[d]++
				e.sum += float64(d)
				e.result.Latency.Reads++
			}
		}
		for e.arrived < len(e.reads) && e.reads[e.arrived].At <= e.now {
			e.arrived++
		}
		for len(e.jobs) < e.cfg.Slots {
			ready, depsDone := e.copyReady()
			canCopy := depsDone && ready <= e.now && e.running < e.cfg.MigrationParallelism

			canRead := e.head < e.arrived
			if !canCopy && !canRead {
				break
			}
			e.order++
			j := completion{order: e.order}
			if canCopy && (!canRead || ready < e.reads[e.head].At) {
				a := e.pending[0]
				e.pending = e.pending[1:]
				j.action = &a
				source, write := e.cfg.ColdReadMs, e.cfg.HotWriteMs
				if a.rec.SourceClass == model.StorageClassHot {
					source, write = e.cfg.HotReadMs, e.cfg.ColdWriteMs
				}
				j.at = e.now + duration(a.rec.SizeBytes, source+write)
				e.running++
				e.copies += a.rec.SizeBytes
				if a.rec.TargetClass == model.StorageClassHot {
					e.hot += a.rec.SizeBytes
				} else {
					e.cold += a.rec.SizeBytes
				}
			} else {
				r := &e.reads[e.head]
				e.head++
				j.read = r
				ms := e.cfg.ColdReadMs
				if e.placement[r.Object].Class == model.StorageClassHot {
					ms = e.cfg.HotReadMs
					e.hotReads++
				}
				j.at = e.now + duration(r.Bytes, ms)
			}
			heap.Push(&e.jobs, j)
			if err := e.check(); err != nil {
				return err
			}
		}
		if err := e.check(); err != nil {
			return err
		}
	}
}
func (e *Engine) copyReady() (int64, bool) {
	if len(e.pending) == 0 {
		return 0, false
	}
	a := e.pending[0]
	ready := a.ready
	for _, d := range a.rec.DependsOn {
		at, ok := e.done[d]
		if !ok {
			return 0, false
		}
		ready = max(ready, at)
	}
	return ready, true
}
func methodVersion(policy string) string {
	switch policy {
	case "lgbm":
		return "lgbm-hurdle-v1"
	case "ewma":
		return "ewma-1h-v1"
	default:
		return "baseline-" + policy + "-v1"
	}
}
func duration(bytes int64, ms float64) int64 {
	return max(1, int64(math.Round(float64(bytes)/model.MiB*ms*float64(time.Millisecond))))
}
func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
func (e *Engine) Finish() (Result, error) {
	if err := e.Advance(math.MaxInt64); err != nil {
		return Result{}, err
	}
	if e.head != len(e.reads) || len(e.jobs) != 0 || len(e.pending) != 0 || e.result.Latency.Reads != int64(len(e.reads)) {
		return Result{}, fmt.Errorf("sim invariant: unfinished read or migration")
	}
	r := e.result
	if r.Latency.Reads == 0 {
		return r, fmt.Errorf("no completed reads")
	}
	r.Latency.MeanMs = e.sum / float64(r.Latency.Reads) / float64(time.Millisecond)
	r.HotShare = float64(e.hotReads) / float64(r.Latency.Reads)
	keys := make([]int64, 0, len(e.histogram))
	for ns := range e.histogram {
		keys = append(keys, ns)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	q := func(p float64) float64 {
		rank := int64(math.Ceil(p * float64(r.Latency.Reads)))
		var count int64
		for _, ns := range keys {
			count += e.histogram[ns]
			if count >= rank {
				return float64(ns) / float64(time.Millisecond)
			}
		}
		return 0
	}
	r.Latency.P50Ms = q(.5)
	r.Latency.P95Ms = q(.95)
	r.Latency.P99Ms = q(.99)
	return r, nil
}
