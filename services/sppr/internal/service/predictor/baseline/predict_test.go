package baseline

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func TestPredict(t *testing.T) {
	x := 2.0
	m := Map{Version: MapVersion, Feature: "age_seconds", RulesVersion: model.FeatureRulesVersion, HorizonSeconds: 3600, Edges: []float64{2}, Values: []float64{8, 4}, Missing: 1}
	for _, tt := range []struct {
		name, kind string
		mapping    Map
		h          time.Duration
		rules      string
		reads      int64
		age        float64
		last       *float64
		want       float64
		bad        bool
	}{
		{"frequency horizon", "frequency", Map{}, 2 * time.Hour, model.FeatureRulesVersion, 48, 0, nil, 4, false},
		{"age edge goes right", "age", m, time.Hour, model.FeatureRulesVersion, 0, 2, nil, 4, false},
		{"age below edge", "age", m, time.Hour, model.FeatureRulesVersion, 0, 1, nil, 8, false},
		{"negative frequency", "frequency", Map{}, time.Hour, model.FeatureRulesVersion, -1, 0, nil, 0, true},
		{"wrong rules", "age", m, time.Hour, "old", 0, 0, nil, 0, true},
		{"zero horizon", "age", m, 0, model.FeatureRulesVersion, 0, 0, nil, 0, true},
		{"infinite feature", "age", m, time.Hour, model.FeatureRulesVersion, 0, math.Inf(1), nil, 0, true},
		{"recency missing", "recency", func() Map { r := m; r.Feature = "last_read_seconds"; return r }(), time.Hour, model.FeatureRulesVersion, 0, 0, nil, 1, false},
		{"recency value", "recency", func() Map { r := m; r.Feature = "last_read_seconds"; return r }(), time.Hour, model.FeatureRulesVersion, 0, 0, &x, 4, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p, err := New(tt.kind, tt.mapping)
			require.NoError(t, err)
			f, err := p.Predict(model.FeatureSnapshot{At: time.Unix(1, 0), RulesVersion: tt.rules, Features: model.Features{Reads24h: tt.reads, AgeSeconds: tt.age, LastReadSeconds: tt.last}}, tt.h)
			if tt.bad {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, f.ExpectedReads)
			}
		})
	}
}
func TestFit(t *testing.T) {
	for _, tt := range []struct {
		name    string
		s       []Sample
		want    []float64
		missing float64
	}{
		{"increasing", []Sample{{0, 1}, {1, 2}, {2, 3}}, []float64{1, 2, 3}, 0},
		{"decreasing", []Sample{{0, 3}, {1, 2}, {2, 1}}, []float64{3, 2, 1}, 0},
		{"ties and missing", []Sample{{0, 1}, {0, 3}, {math.NaN(), 7}}, []float64{2, 2}, 7},
		{"pooled violation", []Sample{{0, 1}, {1, 4}, {2, 3}, {3, 5}}, []float64{1, 3.5, 3.5, 5}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Fit("age_seconds", tt.s)
			require.NoError(t, err)
			j := 0
			for _, s := range tt.s {
				if math.IsNaN(s.X) {
					continue
				}
				require.InDelta(t, tt.want[j], m.Value(&s.X), 1e-6)
				j++
			}
			require.Equal(t, tt.missing, m.Missing)
			path := filepath.Join(t.TempDir(), "baseline-map.json")
			require.NoError(t, m.Save(path))
			require.Error(t, m.Save(path))
			got, err := Load(path)
			require.NoError(t, err)
			require.Equal(t, m, got)
		})
	}
}
func TestInvalidMap(t *testing.T) {
	m, _ := Fit("age_seconds", []Sample{{0, 1}, {1, 2}})
	for _, tt := range []struct {
		name   string
		mutate func(*Map)
	}{
		{"rules", func(m *Map) { m.RulesVersion = "old" }}, {"horizon", func(m *Map) { m.HorizonSeconds = 1 }}, {"negative", func(m *Map) { m.Missing = -1 }}, {"shape", func(m *Map) { m.Values = nil }}, {"monotonicity", func(m *Map) { m.Edges = []float64{1, 2}; m.Values = []float64{1, 3, 2} }},
	} {
		t.Run(tt.name, func(t *testing.T) { copy := m; tt.mutate(&copy); require.Error(t, copy.Validate()) })
	}
	_, err := Fit("age_seconds", nil)
	require.Error(t, err)
	_, err = Fit("age_seconds", []Sample{{0, -1}})
	require.Error(t, err)
}

func TestPythonMonotoneMapParity(t *testing.T) {
	b, err := os.ReadFile("testdata/python-maps.json")
	require.NoError(t, err)
	var cases []struct {
		Name                 string
		X                    []*float64
		Reads, Edges, Values []float64
		Missing              float64
	}
	require.NoError(t, json.Unmarshal(b, &cases))
	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			samples := make([]Sample, len(tt.X))
			for i, x := range tt.X {
				samples[i] = Sample{X: math.NaN(), Reads: tt.Reads[i]}
				if x != nil {
					samples[i].X = *x
				}
			}
			m, err := Fit("age_seconds", samples)
			require.NoError(t, err)
			require.InDeltaSlice(t, tt.Edges, m.Edges, 1e-12)
			require.InDeltaSlice(t, tt.Values, m.Values, 1e-9)
			require.Equal(t, tt.Missing, m.Missing)
		})
	}
}
