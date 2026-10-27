package dataset

import (
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

// Periods are fixed before rows are produced. Labels cannot cross their ends.
type Periods struct {
	Train      model.Interval `json:"train"`
	Validation model.Interval `json:"validation"`
	Test       model.Interval `json:"test"`
}

func DefaultPeriods(start time.Time) Periods {
	return Periods{model.Interval{Start: start, End: start.Add(48 * time.Hour)}, model.Interval{Start: start.Add(48 * time.Hour), End: start.Add(72 * time.Hour)}, model.Interval{Start: start.Add(72 * time.Hour), End: start.Add(168 * time.Hour)}}
}

func (p Periods) Validate() error {
	if p.Train.Start.IsZero() || !p.Train.End.Equal(p.Validation.Start) || !p.Validation.End.Equal(p.Test.Start) {
		return fmt.Errorf("periods must be contiguous")
	}
	for _, interval := range []model.Interval{p.Train, p.Validation, p.Test} {
		if interval.End.Sub(interval.Start) < Horizon {
			return fmt.Errorf("each period must contain at least one label horizon")
		}
	}
	return nil
}

func (p Periods) Split(at time.Time) string {
	for _, interval := range []struct {
		name   string
		bounds model.Interval
	}{{"train", p.Train}, {"validation", p.Validation}, {"test", p.Test}} {
		if !at.Before(interval.bounds.Start) && !at.Add(Horizon).After(interval.bounds.End) {
			return interval.name
		}
	}
	return "purged"
}

func (p Periods) Fractions() map[string]float64 {
	total := p.Test.End.Sub(p.Train.Start).Seconds()
	return map[string]float64{"train": p.Train.End.Sub(p.Train.Start).Seconds() / total, "validation": p.Validation.End.Sub(p.Validation.Start).Seconds() / total, "test": p.Test.End.Sub(p.Test.Start).Seconds() / total}
}

type Options struct {
	Format        string
	SnapshotEvery time.Duration
	TrainEvery    time.Duration
	Periods       Periods
}

func (o Options) normalized(start time.Time, snapshot time.Duration) (Options, error) {
	if o.Format == "" {
		o.Format = "jsonl"
	}
	if o.Format != "jsonl" && o.Format != "csv.gz" {
		return o, fmt.Errorf("format must be jsonl or csv.gz")
	}
	if o.SnapshotEvery == 0 {
		o.SnapshotEvery = snapshot
	}
	if o.SnapshotEvery <= 0 || o.SnapshotEvery > Horizon {
		return o, fmt.Errorf("snapshot interval must be in (0,1h]")
	}
	if o.TrainEvery < 0 || o.TrainEvery != 0 && (o.TrainEvery < o.SnapshotEvery || o.TrainEvery%o.SnapshotEvery != 0) {
		return o, fmt.Errorf("train-every must be a positive multiple of snapshot interval")
	}
	if o.Periods.Train.Start.IsZero() {
		o.Periods = DefaultPeriods(start)
	}
	return o, o.Periods.Validate()
}
