package planner

import (
	"fmt"
	"math"
	"sort"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

// candidate holds the values of the thesis placement model for one version.
type candidate struct {
	item     *model.PlanItem
	reads    float64
	readMiB  float64
	coldMs   float64
	hotMs    float64
	gain     float64
	density  float64
	selected bool
}

type planState struct {
	params     model.PlannerParams
	newID      func() string
	hotUsable  int64
	coldUsable int64
	hotUsed    int64
	coldUsed   int64
	budget     int64
	percentile func(float64) float64
	method     string
	result     []model.Recommendation
	rejected   []rejection
	netGain    float64
	copyBytes  int64
}

type rejection struct {
	rec     model.Recommendation
	density float64
}

// Build is the greedy plan of the thesis: it maximises the net gain G(h) under
// the HOT/COLD capacity and copy budget constraints. For equal sizes and a zero
// threshold the plan is optimal; otherwise it is a heuristic, which tests check
// against exhaustive search on small inputs.
func Build(in model.PlanInput, newID func() string) (model.PlanResult, error) {
	p := in.Params
	if err := validate(in); err != nil {
		return model.PlanResult{}, err
	}
	s := &planState{
		params:     p,
		newID:      newID,
		hotUsable:  usable(in.Hot, p.HotUtilization),
		coldUsable: usable(in.Cold, p.ColdUtilization),
		hotUsed:    in.Hot.UsedBytes,
		coldUsed:   in.Cold.UsedBytes,
		budget:     in.BudgetBytes,
		method:     in.MethodVersion,
	}
	cands := make([]*candidate, len(in.Items))
	var demand []float64
	for i := range in.Items {
		cands[i] = s.evaluate(&in.Items[i])
		if in.Items[i].IsCurrent && in.Items[i].Forecast != nil {
			demand = append(demand, cands[i].reads)
		}
	}
	sort.Float64s(demand)
	s.percentile = func(reads float64) float64 {
		if len(demand) == 0 {
			return 0
		}
		return 100 * float64(sort.SearchFloat64s(demand, reads)) / float64(len(demand))
	}

	hot := filter(cands, func(c *candidate) bool {
		return c.item.Class == model.StorageClassHot && c.item.Blocker == model.BlockerNone
	})
	sort.SliceStable(hot, func(i, j int) bool { return evictBefore(hot[i], hot[j]) })

	s.rebalance(hot)
	promotions := s.candidates(cands)
	s.promote(promotions, hot)
	s.evictNoncurrent(hot)
	return s.finish(len(promotions)), nil
}

func validate(in model.PlanInput) error {
	p := in.Params
	switch {
	case p.Horizon <= 0:
		return fmt.Errorf("horizon must be positive")
	case p.HotMiBPerMs <= 0 || p.ColdMiBPerMs <= 0:
		return fmt.Errorf("transfer speeds must be positive")
	case p.HotFixedMs < 0 || p.ColdFixedMs < 0 || p.PenaltyMsPerMiB < 0 || p.MinGainMs < 0:
		return fmt.Errorf("latency coefficients, penalty and threshold must not be negative")
	case p.HotUtilization <= 0 || p.HotUtilization > 1 || p.ColdUtilization <= 0 || p.ColdUtilization > 1:
		return fmt.Errorf("utilization must be in (0, 1]")
	case in.BudgetBytes < 0:
		return fmt.Errorf("budget must not be negative")
	}
	seen := make(map[string]bool, len(in.Items))
	for _, it := range in.Items {
		if it.VersionID == "" || seen[it.VersionID] {
			return fmt.Errorf("empty or duplicate version %q", it.VersionID)
		}
		seen[it.VersionID] = true
		if !it.Class.Valid() || it.SizeBytes < 0 || it.Generation < 1 {
			return fmt.Errorf("invalid placement of version %s", it.VersionID)
		}
	}
	return nil
}

func usable(c model.ClassState, planner float64) int64 {
	u := planner
	if c.MaxUtilization > 0 && c.MaxUtilization < u {
		u = c.MaxUtilization
	}
	return int64(math.Floor(u * float64(c.CapacityBytes)))
}

func (s *planState) evaluate(it *model.PlanItem) *candidate {
	c := &candidate{item: it}
	if it.IsCurrent && it.Forecast != nil {
		r := it.ReadFraction
		if r <= 0 || r > 1 {
			r = 1
		}
		c.reads = it.Forecast.ExpectedReads
		c.readMiB = c.reads * r * float64(it.SizeBytes) / model.MiB
	}
	c.coldMs = c.reads*s.params.ColdFixedMs + c.readMiB/s.params.ColdMiBPerMs
	c.hotMs = c.reads*s.params.HotFixedMs + c.readMiB/s.params.HotMiBPerMs
	c.gain = c.coldMs - c.hotMs
	// A zero-size version costs nothing to move; one byte keeps the ratio finite.
	c.density = c.gain / math.Max(float64(it.SizeBytes), 1)
	return c
}

// evictBefore orders HOT versions for displacement: lowest gain per byte first,
// overwritten versions before current ones at equal gain.
func evictBefore(a, b *candidate) bool {
	if a.density != b.density {
		return a.density < b.density
	}
	if a.item.IsCurrent != b.item.IsCurrent {
		return !a.item.IsCurrent
	}
	return a.item.VersionID < b.item.VersionID
}

func filter(cands []*candidate, keep func(*candidate) bool) []*candidate {
	var out []*candidate
	for _, c := range cands {
		if keep(c) {
			out = append(out, c)
		}
	}
	return out
}

// rebalance brings HOT back under its allowed utilization before anything is
// promoted; the threshold does not apply, because the plan must be feasible.
func (s *planState) rebalance(hot []*candidate) {
	for _, c := range hot {
		if s.hotUsed <= s.hotUsable {
			return
		}
		size := c.item.SizeBytes
		if size > s.budget || s.coldUsed+size > s.coldUsable {
			continue
		}
		before := s.snapshot()
		s.hotUsed -= size
		s.coldUsed += size
		s.budget -= size
		penalty := s.params.PenaltyMsPerMiB * float64(size) / model.MiB
		net := -c.gain - penalty
		s.add(c, model.KindRebalance, nil, before, model.EffectExplanation{
			GainMs: c.gain, SetGainMs: -c.gain, SetPenaltyMs: penalty, SetNetGainMs: net, MinGainMs: s.params.MinGainMs,
		}, "HOT is above its allowed utilization")
		s.netGain += net
	}
}

// candidates are current COLD versions in order of gain per byte; blocked and
// unforecast versions with a positive gain are recorded as rejected.
func (s *planState) candidates(cands []*candidate) []*candidate {
	var out []*candidate
	for _, c := range cands {
		it := c.item
		if it.Class != model.StorageClassCold || !it.IsCurrent {
			continue
		}
		if it.Forecast == nil {
			s.reject(c, model.KindPromote, model.RejectNoForecast, "no usable forecast for this version")
			continue
		}
		if c.gain <= 0 {
			continue
		}
		if it.Blocker != model.BlockerNone {
			s.reject(c, model.KindPromote, model.RejectReason(it.Blocker), "")
			continue
		}
		out = append(out, c)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].density != out[j].density {
			return out[i].density > out[j].density
		}
		return out[i].item.VersionID < out[j].item.VersionID
	})
	return out
}

// swap is one way to make room for a promotion: what it displaces and the
// promotions that share the freed space.
type swap struct {
	displaced []*candidate
	group     []*candidate
	freed     int64
	copyBytes int64
	effect    model.EffectExplanation
	reason    model.RejectReason
}

func (s *planState) promote(promotions, hot []*candidate) {
	for i, c := range promotions {
		if c.selected {
			continue
		}
		size := c.item.SizeBytes
		if s.budget <= 0 || size > s.budget {
			s.reject(c, model.KindPromote, model.RejectBudget, "")
			continue
		}
		free := s.hotUsable - s.hotUsed
		var options []*swap
		for _, displaced := range s.displacements(hot, free, size) {
			options = append(options, s.evaluateSwap(c, promotions[i+1:], displaced, free))
		}
		if len(options) == 0 {
			s.reject(c, model.KindPromote, model.RejectHotCapacity, "")
			continue
		}
		best := options[0]
		for _, o := range options[1:] {
			if o.reason == model.RejectNone && (best.reason != model.RejectNone || o.effect.SetNetGainMs > best.effect.SetNetGainMs) {
				best = o
			}
		}
		if best.reason != model.RejectNone {
			s.rejectWith(c, model.KindPromote, best.reason, best.effect, best.displaced)
			continue
		}
		s.apply(best)
	}
}

// displacements returns the sets of HOT versions that could make room: the
// cheapest per byte that fit the budget, and the single version with the least
// gain that frees enough alone. Without the second, a small promotion could
// displace a long run of low-density versions it does not need.
func (s *planState) displacements(hot []*candidate, free, size int64) [][]*candidate {
	if free >= size {
		return [][]*candidate{nil}
	}
	var out [][]*candidate
	var byDensity []*candidate
	var freed int64
	for _, h := range hot {
		if free+freed >= size {
			break
		}
		if h.selected || size+freed+h.item.SizeBytes > s.budget {
			continue
		}
		byDensity = append(byDensity, h)
		freed += h.item.SizeBytes
	}
	if free+freed >= size {
		out = append(out, byDensity)
	}
	var single *candidate
	for _, h := range hot {
		if h.selected || free+h.item.SizeBytes < size {
			continue
		}
		if single == nil || h.gain < single.gain || (h.gain == single.gain && h.item.SizeBytes < single.item.SizeBytes) {
			single = h
		}
	}
	if single != nil && !(len(byDensity) == 1 && byDensity[0] == single) {
		out = append(out, []*candidate{single})
	}
	return out
}

func (s *planState) evaluateSwap(c *candidate, rest, displaced []*candidate, free int64) *swap {
	o := &swap{displaced: displaced, group: []*candidate{c}}
	var lost float64
	for _, h := range displaced {
		o.freed += h.item.SizeBytes
		lost += h.gain
	}
	size := c.item.SizeBytes
	if len(displaced) > 0 {
		o.group = append(o.group, s.followers(rest, free+o.freed-size, size+o.freed)...)
	}
	var gained float64
	var promoted int64
	for _, g := range o.group {
		gained += g.gain
		promoted += g.item.SizeBytes
	}
	o.copyBytes = promoted + o.freed
	setGain := gained - lost
	penalty := s.params.PenaltyMsPerMiB * float64(o.copyBytes) / model.MiB
	o.effect = model.EffectExplanation{
		GainMs: c.gain, SetGainMs: setGain, SetPenaltyMs: penalty, SetNetGainMs: setGain - penalty, MinGainMs: s.params.MinGainMs,
	}
	switch {
	case o.effect.SetNetGainMs < s.params.MinGainMs:
		o.reason = model.RejectGainThreshold
	case o.copyBytes > s.budget:
		o.reason = model.RejectBudget
	case s.coldUsed+o.freed > s.coldUsable:
		o.reason = model.RejectColdCapacity
	}
	return o
}

func (s *planState) apply(o *swap) {
	set := s.newID()
	ids := make([]string, 0, len(o.displaced))
	for _, h := range o.displaced {
		before := s.snapshot()
		s.hotUsed -= h.item.SizeBytes
		s.coldUsed += h.item.SizeBytes
		s.budget -= h.item.SizeBytes
		e := o.effect
		e.Set, e.GainMs = set, h.gain
		ids = append(ids, s.add(h, model.KindEvict, nil, before, e, "frees HOT for versions with a higher gain"))
	}
	for _, g := range o.group {
		e := o.effect
		e.Set, e.GainMs = set, g.gain
		e.Displaced = displacedExplanation(o.displaced, ids)
		before := s.snapshot()
		s.hotUsed += g.item.SizeBytes
		s.budget -= g.item.SizeBytes
		s.add(g, model.KindPromote, ids, before, e, "")
	}
	s.netGain += o.effect.SetNetGainMs
}

// followers fill the HOT space a displacement frees beyond the leader's size,
// so one large cold-in-HOT version can be traded for several small hot ones.
// Each follower must pay for its own copy; the set still faces the threshold.
func (s *planState) followers(rest []*candidate, space, copyBytes int64) []*candidate {
	var out []*candidate
	for _, f := range rest {
		size := f.item.SizeBytes
		if f.selected || size > space || copyBytes+size > s.budget {
			continue
		}
		if f.gain-s.params.PenaltyMsPerMiB*float64(size)/model.MiB <= 0 {
			continue
		}
		out = append(out, f)
		space -= size
		copyBytes += size
	}
	return out
}

// evictNoncurrent spends what is left of the budget on overwritten versions
// still in HOT: nobody reads them, so HOT space is the only thing they hold.
func (s *planState) evictNoncurrent(hot []*candidate) {
	for _, c := range hot {
		size := c.item.SizeBytes
		if c.selected || c.item.IsCurrent || size > s.budget || s.coldUsed+size > s.coldUsable {
			continue
		}
		before := s.snapshot()
		s.hotUsed -= size
		s.coldUsed += size
		s.budget -= size
		penalty := s.params.PenaltyMsPerMiB * float64(size) / model.MiB
		s.add(c, model.KindEvictNoncurrent, nil, before, model.EffectExplanation{
			SetPenaltyMs: penalty, SetNetGainMs: -penalty, MinGainMs: s.params.MinGainMs,
		}, "overwritten version; its forecast is 0")
		s.netGain -= penalty
	}
}

func (s *planState) snapshot() model.ResourceExplanation {
	return model.ResourceExplanation{
		HotUsedBefore: s.hotUsed, HotUsable: s.hotUsable,
		ColdUsedBefore: s.coldUsed, ColdUsable: s.coldUsable,
		BudgetBefore: s.budget,
	}
}

func (s *planState) add(c *candidate, kind model.RecommendationKind, deps []string, res model.ResourceExplanation, effect model.EffectExplanation, note string) string {
	c.selected = true
	res.HotUsedAfter, res.ColdUsedAfter, res.BudgetAfter = s.hotUsed, s.coldUsed, s.budget
	res.CopyBytes = c.item.SizeBytes
	s.copyBytes += c.item.SizeBytes
	rec := s.recommendation(c, kind)
	rec.Selected = true
	rec.DependsOn = deps
	effect.ColdLatencyMs, effect.HotLatencyMs = c.coldMs, c.hotMs
	rec.Explanation = model.Explanation{Forecast: s.forecast(c), Effect: effect, Resources: res, DependsOn: deps, Note: note}
	rec.Order = len(s.result)
	s.result = append(s.result, rec)
	return rec.RecommendationID
}

func (s *planState) reject(c *candidate, kind model.RecommendationKind, reason model.RejectReason, note string) {
	s.rejectWith(c, kind, reason, model.EffectExplanation{
		GainMs: c.gain, SetGainMs: c.gain, MinGainMs: s.params.MinGainMs,
		SetPenaltyMs: s.params.PenaltyMsPerMiB * float64(c.item.SizeBytes) / model.MiB,
		SetNetGainMs: c.gain - s.params.PenaltyMsPerMiB*float64(c.item.SizeBytes)/model.MiB,
	}, nil)
	s.rejected[len(s.rejected)-1].rec.Explanation.Note = note
}

func (s *planState) rejectWith(c *candidate, kind model.RecommendationKind, reason model.RejectReason, effect model.EffectExplanation, displaced []*candidate) {
	effect.Displaced = displacedExplanation(displaced, nil)
	rec := s.recommendation(c, kind)
	rec.Reason = reason
	res := s.snapshot()
	res.HotUsedAfter, res.ColdUsedAfter, res.BudgetAfter = s.hotUsed, s.coldUsed, s.budget
	res.CopyBytes = c.item.SizeBytes
	effect.ColdLatencyMs, effect.HotLatencyMs = c.coldMs, c.hotMs
	rec.Explanation = model.Explanation{Forecast: s.forecast(c), Effect: effect, Resources: res, Reason: reason}
	s.rejected = append(s.rejected, rejection{rec: rec, density: c.density})
}

func (s *planState) recommendation(c *candidate, kind model.RecommendationKind) model.Recommendation {
	it := c.item
	target := model.StorageClassCold
	if kind == model.KindPromote {
		target = model.StorageClassHot
	}
	return model.Recommendation{
		RecommendationID: s.newID(), Kind: kind,
		VersionID: it.VersionID, Bucket: it.Bucket, Key: it.Key,
		ExpectedGeneration: it.Generation, SourceClass: it.Class, TargetClass: target, SizeBytes: it.SizeBytes,
	}
}

func (s *planState) forecast(c *candidate) model.ForecastExplanation {
	it := c.item
	f := model.ForecastExplanation{Method: s.method, HorizonSeconds: int64(s.params.Horizon.Seconds())}
	switch {
	case !it.IsCurrent:
		f.Quality = "noncurrent"
		return f
	case it.Forecast == nil:
		f.Quality = "unavailable"
		return f
	}
	f.ExpectedReads = c.reads
	f.ReadFraction = it.ReadFraction
	f.ExpectedMiB = c.readMiB
	f.Quality = it.Forecast.Quality.Reason
	f.Percentile = s.percentile(c.reads)
	if ft := it.Features; ft != nil {
		f.LastReadSeconds = ft.LastReadSeconds
		f.Reads5m, f.Reads1h, f.Reads6h, f.Reads24h = ft.Reads5m, ft.Reads1h, ft.Reads6h, ft.Reads24h
		f.EWMA1h = ft.EWMA1h
	}
	return f
}

func displacedExplanation(displaced []*candidate, ids []string) []model.DisplacedExplanation {
	out := make([]model.DisplacedExplanation, 0, len(displaced))
	for i, h := range displaced {
		d := model.DisplacedExplanation{VersionID: h.item.VersionID, SizeBytes: h.item.SizeBytes, GainMs: h.gain}
		if ids != nil {
			d.RecommendationID = ids[i]
		}
		out = append(out, d)
	}
	return out
}

func (s *planState) finish(candidates int) model.PlanResult {
	sort.SliceStable(s.rejected, func(i, j int) bool { return s.rejected[i].density > s.rejected[j].density })
	kept := len(s.rejected)
	if max := s.params.MaxRejectedSaved; max >= 0 && kept > max {
		kept = max
	}
	recs := s.result
	for _, r := range s.rejected[:kept] {
		r.rec.Order = len(recs)
		recs = append(recs, r.rec)
	}
	return model.PlanResult{
		Recommendations: recs,
		Summary: model.PlanSummary{
			Actions: len(s.result), CopyBytes: s.copyBytes, NetGainMs: s.netGain,
			Candidates: candidates, Rejected: len(s.rejected), RejectedKept: kept,
		},
	}
}
