package features

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
)

const MaxWindow = 24 * time.Hour

type builder struct{}

func NewService() service.FeatureBuilder { return &builder{} }

type read struct {
	at    time.Time
	bytes int64
}

// Build is the single calculation used by dataset preparation and prediction.
// No placement, generator group or future popularity is part of its input schema.
func (*builder) Build(ctx context.Context, req model.FeatureRequest) ([]model.FeatureSnapshot, error) {
	if req.At.IsZero() {
		return nil, fmt.Errorf("snapshot time is required")
	}
	if err := ValidateCoverage(req.Coverage); err != nil {
		return nil, err
	}
	history := make(map[model.VersionRef][]read)
	bucketReads := make(map[string]int64)
	for i, e := range req.Events {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if !e.IsDemand() || !e.OccurredAt.Before(req.At) {
			continue
		}
		if e.OccurredAt.IsZero() || e.Bytes < 0 {
			return nil, fmt.Errorf("invalid demand event")
		}
		ref := model.VersionRef{Bucket: e.Bucket, ObjectKey: e.ObjectKey, VersionID: e.VersionID}
		history[ref] = append(history[ref], read{e.OccurredAt, e.Bytes})
		if !e.OccurredAt.Before(req.At.Add(-time.Hour)) {
			bucketReads[e.Bucket]++
		}
	}
	for ref := range history {
		sort.Slice(history[ref], func(i, j int) bool { return history[ref][i].at.Before(history[ref][j].at) })
	}
	seen := make(map[model.VersionRef]bool)
	created := make(map[[2]string][]time.Time)
	catalogStart := req.At
	for _, v := range req.Catalog {
		if v.Bucket == "" || v.ObjectKey == "" || v.VersionID == "" || v.SizeBytes < 0 || v.CreatedAt.IsZero() || (v.DeletedAt != nil && v.DeletedAt.Before(v.CreatedAt)) {
			return nil, fmt.Errorf("invalid catalogue version %v", v.VersionRef)
		}
		if seen[v.VersionRef] {
			return nil, fmt.Errorf("duplicate catalogue version %v", v.VersionRef)
		}
		seen[v.VersionRef] = true
		if v.CreatedAt.Before(catalogStart) {
			catalogStart = v.CreatedAt
		}
		// Creation at T is visible as metadata, but never contributes a past overwrite.
		if v.CreatedAt.Before(req.At) {
			key := [2]string{v.Bucket, v.ObjectKey}
			created[key] = append(created[key], v.CreatedAt)
		}
	}
	bucketCount, bucketActive := make(map[string]int), make(map[string]int)
	result := make([]model.FeatureSnapshot, 0, len(req.Catalog))
	for _, v := range req.Catalog {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !v.Active(req.At) {
			continue
		}
		f := model.Features{LogSize: math.Log1p(float64(v.SizeBytes)), AgeSeconds: req.At.Sub(v.CreatedAt).Seconds()}
		for _, at := range created[[2]string{v.Bucket, v.ObjectKey}] {
			if !at.Before(req.At.Add(-MaxWindow)) {
				// The first known creation is not an overwrite.
				for _, prior := range created[[2]string{v.Bucket, v.ObjectKey}] {
					if prior.Before(at) {
						f.Overwrites24h++
						break
					}
				}
			}
		}
		rs := history[v.VersionRef]
		for i := len(rs) - 1; i >= 0; i-- {
			r := rs[i]
			age := req.At.Sub(r.at)
			if i == len(rs)-1 {
				seconds := age.Seconds()
				f.LastReadSeconds = &seconds
			}
			if i == len(rs)-2 {
				seconds := age.Seconds()
				f.PreviousReadSeconds = &seconds
			}
			if r.at.Before(v.CreatedAt) || (v.DeletedAt != nil && !r.at.Before(*v.DeletedAt)) {
				return nil, fmt.Errorf("read outside version lifetime: %v", v.VersionRef)
			}
			if age > MaxWindow {
				continue
			}
			if r.bytes > v.SizeBytes {
				return nil, fmt.Errorf("read exceeds version size: %v", v.VersionRef)
			}
			f.Reads24h++
			if age <= 6*time.Hour {
				f.Reads6h++
			}
			if age <= time.Hour {
				f.Reads1h++
			}
			if age <= 5*time.Minute {
				f.Reads5m++
			}
			if age > 23*time.Hour {
				f.ReadsDayAgo++
			}
			f.EWMA1h += math.Ln2 * math.Exp(-math.Ln2*age.Hours())
			f.EWMA6h += (math.Ln2 / 6) * math.Exp(-math.Ln2*age.Hours()/6)
			if v.SizeBytes > 0 {
				f.MeanReadFraction += float64(r.bytes) / float64(v.SizeBytes)
			}
		}
		if f.Reads24h > 0 {
			f.MeanReadFraction /= float64(f.Reads24h)
		}
		if f.Reads1h > 0 {
			f.ShortLongRatio = 12 * float64(f.Reads5m) / float64(f.Reads1h)
		}
		start := req.At.Add(-MaxWindow)
		if v.CreatedAt.After(start) {
			start = v.CreatedAt
		}
		if req.Coverage.Start.After(start) {
			start = req.Coverage.Start
		}
		if start.Before(req.At) {
			f.ObservedSeconds = req.At.Sub(start).Seconds()
		}
		// Bucket features share history across versions, so even a gap before
		// this version's birth makes the combined vector incomplete.
		qualityStart := req.At.Add(-MaxWindow)
		if catalogStart.After(qualityStart) {
			qualityStart = catalogStart
		}
		quality := Quality(req.Coverage, qualityStart, req.At)
		if quality.Usable && f.ObservedSeconds == 0 {
			quality = model.DataQuality{Usable: v.CreatedAt.Equal(req.At), Reason: "no_history"}
		} else if quality.Usable && f.ObservedSeconds < MaxWindow.Seconds() {
			quality.Reason = "warmup"
		}
		bucketCount[v.Bucket]++
		if f.Reads1h > 0 {
			bucketActive[v.Bucket]++
		}
		result = append(result, model.FeatureSnapshot{VersionRef: v.VersionRef, At: req.At.UTC(), SizeBytes: v.SizeBytes, RulesVersion: model.FeatureRulesVersion, Features: f, Quality: quality})
	}
	for i := range result {
		b := result[i].Bucket
		result[i].Features.BucketReads1h = bucketReads[b]
		result[i].Features.BucketActiveFraction = float64(bucketActive[b]) / float64(bucketCount[b])
	}
	return result, nil
}

func ValidateCoverage(c model.Coverage) error {
	if c.Known && (c.Start.IsZero() || c.Watermark.Before(c.Start)) {
		return fmt.Errorf("invalid coverage bounds")
	}
	for _, g := range c.Gaps {
		if g.Start.IsZero() || !g.End.After(g.Start) {
			return fmt.Errorf("invalid coverage gap")
		}
	}
	return nil
}

func Quality(c model.Coverage, start, end time.Time) model.DataQuality {
	reason := "complete"
	switch {
	case !c.Known:
		reason = "unknown_completeness"
	case c.Start.After(start) || c.Start.After(end):
		reason = "unobserved"
	case c.Watermark.Before(end):
		reason = "watermark_behind"
	default:
		for _, g := range c.Gaps {
			if g.Start.Before(end) && g.End.After(start) {
				return model.DataQuality{Reason: "gap"}
			}
		}
		return model.DataQuality{Usable: true, Reason: reason}
	}
	return model.DataQuality{Reason: reason}
}
