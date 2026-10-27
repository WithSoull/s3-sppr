package synthetic

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

const GeneratorVersion = "synthetic-v1"

var Scenarios = []string{"stable", "decay", "periodic", "burst", "shift", "uniform"}

type Config struct {
	Scenario           string    `json:"scenario"`
	Seed               uint64    `json:"seed"`
	Start              time.Time `json:"start"`
	Objects            int       `json:"objects"`
	SizeBytes          int64     `json:"size_bytes"`
	ReadsPerSecond     float64   `json:"reads_per_second"`
	SnapshotSeconds    int       `json:"snapshot_seconds"`
	HotFraction        float64   `json:"hot_fraction"`
	HotProbability     float64   `json:"hot_probability"`
	DecayHours         float64   `json:"decay_hours"`
	PeriodHours        float64   `json:"period_hours"`
	BurstAtHours       float64   `json:"burst_at_hours"`
	BurstDurationHours float64   `json:"burst_duration_hours"`
	ShiftEveryHours    float64   `json:"shift_every_hours"`
	Lifecycle          bool      `json:"lifecycle"`
}

func (c Config) End() time.Time { return c.Start.Add(7 * 24 * time.Hour) }

func (c Config) Validate() error {
	validScenario := false
	for _, name := range Scenarios {
		if c.Scenario == name {
			validScenario = true
		}
	}
	if !validScenario || c.Start.IsZero() || c.Objects < 10 || c.Objects > 100000 || c.SizeBytes <= 0 || c.ReadsPerSecond < 1.0/3600 || c.ReadsPerSecond > 10000 || c.SnapshotSeconds < 1 || c.SnapshotSeconds > 3600 || 3600%c.SnapshotSeconds != 0 {
		return fmt.Errorf("invalid scenario, catalogue, rate or snapshot interval")
	}
	for _, x := range []float64{c.ReadsPerSecond, c.HotFraction, c.HotProbability, c.DecayHours, c.PeriodHours, c.BurstAtHours, c.BurstDurationHours, c.ShiftEveryHours} {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return fmt.Errorf("non-finite generator parameter")
		}
	}
	if c.HotFraction <= 0 || c.HotFraction > 0.5 || c.HotProbability < 0 || c.HotProbability > 1 || c.DecayHours <= 0 || c.PeriodHours <= 0 || c.BurstAtHours < 0 || c.BurstDurationHours <= 0 || c.BurstAtHours+c.BurstDurationHours > 168 || c.ShiftEveryHours <= 0 {
		return fmt.Errorf("invalid popularity parameters")
	}
	return nil
}

type Generator struct {
	cfg      Config
	rng      *rand.Rand
	catalog  []model.Version
	sequence int64
	hot      int
	order    []int
}

func New(c Config) (*Generator, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	g := &Generator{cfg: c, rng: rand.New(rand.NewPCG(c.Seed, c.Seed^0x9e3779b97f4a7c15)), hot: max(1, int(float64(c.Objects)*c.HotFraction))}
	g.order = g.rng.Perm(c.Objects)
	for i := 0; i < c.Objects; i++ {
		v := model.Version{VersionRef: model.VersionRef{Bucket: fmt.Sprintf("bucket-%02d", i%4), ObjectKey: fmt.Sprintf("object-%05d", i), VersionID: fmt.Sprintf("v-%05d", i)}, SizeBytes: c.SizeBytes, CreatedAt: c.Start}
		if c.Lifecycle {
			if i%10 == 0 {
				v.CreatedAt = c.Start.Add(24 * time.Hour)
			}
			if i%10 == 1 {
				deleted := c.Start.Add(96 * time.Hour)
				v.DeletedAt = &deleted
			}
		}
		g.catalog = append(g.catalog, v)
	}
	return g, nil
}

func (g *Generator) Catalog() []model.Version { return append([]model.Version(nil), g.catalog...) }

// Next advances logical time; it never sleeps or observes a placement policy.
func (g *Generator) Next() (model.AccessEvent, bool) {
	c := g.cfg
	at := c.Start.Add(time.Duration(math.Round(float64(g.sequence) * float64(time.Second) / c.ReadsPerSecond)))
	if !at.Before(c.End()) {
		return model.AccessEvent{}, false
	}
	hours := at.Sub(c.Start).Hours()
	p := c.HotProbability
	group := 0
	switch c.Scenario {
	case "decay":
		p *= math.Exp(-hours / c.DecayHours)
	case "periodic":
		p *= 0.5 + 0.5*math.Cos(2*math.Pi*hours/c.PeriodHours)
	case "burst":
		if hours < c.BurstAtHours || hours >= c.BurstAtHours+c.BurstDurationHours {
			p = 0
		}
	case "shift":
		group = int(math.Floor(hours/c.ShiftEveryHours)) % (c.Objects / g.hot)
	case "uniform":
		p = 0
	}
	var v model.Version
	for {
		i := g.rng.IntN(c.Objects)
		if g.rng.Float64() < p {
			i = g.order[group*g.hot+g.rng.IntN(g.hot)]
		}
		v = g.catalog[i]
		if v.Active(at) {
			break
		}
	}
	g.sequence++
	return model.AccessEvent{GatewayID: "synthetic", StreamID: fmt.Sprintf("%s-%d", c.Scenario, c.Seed), Sequence: g.sequence, OccurredAt: at, Bucket: v.Bucket, ObjectKey: v.ObjectKey, VersionID: v.VersionID, Operation: model.OperationRead, Origin: model.OriginClient, Result: model.ResultSuccess, Bytes: v.SizeBytes}, true
}
