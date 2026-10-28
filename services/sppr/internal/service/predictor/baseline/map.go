// Package baseline implements the same quantile-binned isotonic estimates as
// sppr_ml.baselines.MonotoneMap. Fitting never sees validation or test labels.
package baseline

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

const MapVersion = "isotonic-quantile50-v1"

type Sample struct{ X, Reads float64 }
type Map struct {
	Version        string    `json:"version"`
	Feature        string    `json:"feature"`
	RulesVersion   string    `json:"feature_rules_version"`
	HorizonSeconds int64     `json:"horizon_seconds"`
	Edges          []float64 `json:"edges"`
	Values         []float64 `json:"values"`
	Missing        float64   `json:"missing"`
}

func Fit(feature string, samples []Sample) (Map, error) {
	m := Map{Version: MapVersion, Feature: feature, RulesVersion: model.FeatureRulesVersion, HorizonSeconds: 3600, Edges: []float64{}}
	if len(samples) == 0 {
		return m, fmt.Errorf("empty training sample")
	}
	finite := make([]Sample, 0, len(samples))
	missing := 0
	for _, s := range samples {
		if !valid(s.Reads) || s.Reads < 0 {
			return m, fmt.Errorf("invalid training label")
		}
		if math.IsNaN(s.X) {
			m.Missing += s.Reads
			missing++
			continue
		}
		if !valid(s.X) || s.X < 0 {
			return m, fmt.Errorf("invalid feature")
		}
		finite = append(finite, s)
	}
	if missing > 0 {
		m.Missing /= float64(missing)
	}
	sort.SliceStable(finite, func(i, j int) bool { return finite[i].X < finite[j].X })
	for b := 1; b < 50 && len(finite) > 0; b++ {
		pos := float64(len(finite)-1) * float64(b) / 50
		lo := int(pos)
		hi := min(lo+1, len(finite)-1)
		edge := finite[lo].X + (finite[hi].X-finite[lo].X)*(pos-float64(lo))
		if len(m.Edges) == 0 || edge != m.Edges[len(m.Edges)-1] {
			m.Edges = append(m.Edges, edge)
		}
	}
	sums := make([]float64, len(m.Edges)+1)
	weights := make([]float64, len(sums))
	for _, s := range finite {
		i := sort.Search(len(m.Edges), func(i int) bool { return m.Edges[i] > s.X })
		sums[i] += s.Reads
		weights[i]++
	}
	// Spearman's sign selects the direction; ties receive their average rank.
	xs := make([]float64, len(finite))
	ys := make([]float64, len(finite))
	for i, s := range finite {
		xs[i] = s.X
		ys[i] = s.Reads
	}
	xr, yr := ranks(xs), ranks(ys)
	mean := float64(len(finite)-1) / 2
	cov := 0.0
	for i := range xr {
		cov += (xr[i] - mean) * (yr[i] - mean)
	}
	sign := 1.0
	if cov < 0 {
		sign = -1
	}
	type block struct {
		sum, weight float64
		n           int
	}
	blocks := []block{}
	for i := range sums {
		w := math.Max(weights[i], 1e-9)
		blocks = append(blocks, block{sign * sums[i], w, 1})
		for len(blocks) > 1 {
			n := len(blocks)
			a, b := blocks[n-2], blocks[n-1]
			if a.sum/a.weight <= b.sum/b.weight {
				break
			}
			blocks[n-2] = block{a.sum + b.sum, a.weight + b.weight, a.n + b.n}
			blocks = blocks[:n-1]
		}
	}
	for _, b := range blocks {
		for j := 0; j < b.n; j++ {
			m.Values = append(m.Values, sign*b.sum/b.weight)
		}
	}
	return m, m.Validate()
}
func ranks(x []float64) []float64 {
	order := make([]int, len(x))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool { return x[order[i]] < x[order[j]] })
	out := make([]float64, len(x))
	for i := 0; i < len(order); {
		j := i + 1
		for j < len(order) && x[order[i]] == x[order[j]] {
			j++
		}
		for k := i; k < j; k++ {
			out[order[k]] = float64(i+j-1) / 2
		}
		i = j
	}
	return out
}
func valid(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
func (m Map) Validate() error {
	if m.Version != MapVersion || m.RulesVersion != model.FeatureRulesVersion || m.HorizonSeconds != 3600 || (m.Feature != "age_seconds" && m.Feature != "last_read_seconds") || len(m.Values) != len(m.Edges)+1 || !valid(m.Missing) || m.Missing < 0 {
		return fmt.Errorf("incompatible baseline map")
	}
	direction := 0
	for i, v := range m.Values {
		if !valid(v) || v < 0 {
			return fmt.Errorf("invalid map value")
		}
		if i > 0 {
			d := 0
			if v > m.Values[i-1] {
				d = 1
			}
			if v < m.Values[i-1] {
				d = -1
			}
			if direction*d < 0 {
				return fmt.Errorf("non-monotone map")
			}
			if d != 0 {
				direction = d
			}
		}
	}
	for i, v := range m.Edges {
		if !valid(v) || v < 0 || i > 0 && v <= m.Edges[i-1] {
			return fmt.Errorf("invalid map edges")
		}
	}
	return nil
}
func (m Map) Value(x *float64) float64 {
	if x == nil || math.IsNaN(*x) {
		return m.Missing
	}
	return m.Values[sort.Search(len(m.Edges), func(i int) bool { return m.Edges[i] > *x })]
}
func (m Map) Save(path string) error {
	if err := m.Validate(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	ce := f.Close()
	if err != nil {
		return err
	}
	return ce
}
func Load(path string) (Map, error) {
	var m Map
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err = json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	return m, m.Validate()
}
