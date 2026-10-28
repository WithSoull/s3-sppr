package baseline

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type Predictor struct {
	kind    string
	mapping Map
	version string
}

func New(kind string, mapping Map) (*Predictor, error) {
	if kind != "frequency" {
		if err := mapping.Validate(); err != nil {
			return nil, err
		}
		want := map[string]string{"age": "age_seconds", "recency": "last_read_seconds"}[kind]
		if want == "" || want != mapping.Feature {
			return nil, fmt.Errorf("map feature does not match %q", kind)
		}
	}
	version := ""
	if kind != "frequency" {
		b, _ := json.Marshal(mapping)
		sum := sha256.Sum256(b)
		version = fmt.Sprintf("%s-%x", MapVersion, sum[:8])
	}
	return &Predictor{kind: kind, mapping: mapping, version: version}, nil
}
func (p *Predictor) MethodVersion() string { return "baseline-" + p.kind + "-v1" }
func (p *Predictor) ModelVersion() string {
	if p.kind == "frequency" {
		return ""
	}
	return p.version
}
func (p *Predictor) Predict(s model.FeatureSnapshot, h time.Duration) (model.DemandForecast, error) {
	if s.RulesVersion != model.FeatureRulesVersion || s.At.IsZero() || h <= 0 || h%time.Second != 0 {
		return model.DemandForecast{}, fmt.Errorf("incompatible snapshot or horizon")
	}
	n := float64(s.Features.Reads24h) / 24
	switch p.kind {
	case "age":
		if !valid(s.Features.AgeSeconds) || s.Features.AgeSeconds < 0 {
			return model.DemandForecast{}, fmt.Errorf("invalid age")
		}
		n = p.mapping.Value(&s.Features.AgeSeconds)
	case "recency":
		x := s.Features.LastReadSeconds
		if x != nil && (!valid(*x) || *x < 0) {
			return model.DemandForecast{}, fmt.Errorf("invalid recency")
		}
		n = p.mapping.Value(x)
	}
	n *= h.Hours()
	if !valid(n) || n < 0 {
		return model.DemandForecast{}, fmt.Errorf("invalid forecast")
	}
	return model.DemandForecast{VersionRef: s.VersionRef, At: s.At, HorizonSeconds: int64(h / time.Second), ExpectedReads: n, Quality: s.Quality, MethodVersion: p.MethodVersion(), ModelVersion: p.ModelVersion(), FeatureRulesVersion: s.RulesVersion}, nil
}
