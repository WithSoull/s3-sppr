package dataset

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
)

type Manifest struct {
	Config              synthetic.Config   `json:"config"`
	GeneratorVersion    string             `json:"generator_version"`
	FeatureRulesVersion string             `json:"feature_rules_version"`
	ConfigSHA256        string             `json:"config_sha256"`
	HorizonSeconds      int64              `json:"horizon_seconds"`
	Train               model.Interval     `json:"train"`
	Validation          model.Interval     `json:"validation"`
	Test                model.Interval     `json:"test"`
	Events              int64              `json:"events"`
	Rows                map[string]int64   `json:"rows"`
	Eligible            map[string]int64   `json:"eligible"`
	SHA256              map[string]string  `json:"sha256"`
	Provenance          map[string]string  `json:"provenance"`
	Format              string             `json:"format"`
	SnapshotSeconds     float64            `json:"snapshot_seconds"`
	TrainEverySeconds   float64            `json:"train_every_seconds"`
	SplitFractions      map[string]float64 `json:"split_fractions"`
	InputSHA256         map[string]string  `json:"input_sha256,omitempty"`
	SourceManifest      json.RawMessage    `json:"source_manifest,omitempty"`
	CompactColumns      []string           `json:"compact_columns,omitempty"`
	CoveragePolicy      string             `json:"coverage_policy"`
}

type jsonLines struct {
	f       *os.File
	buffer  *bufio.Writer
	encoder *json.Encoder
}

func openLines(path string) (*jsonLines, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	b := bufio.NewWriterSize(f, 256*1024)
	return &jsonLines{f: f, buffer: b, encoder: json.NewEncoder(b)}, nil
}
func (w *jsonLines) close() error {
	err := w.buffer.Flush()
	closeErr := w.f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// Write streams output and retains one day of reads plus the last two older
// reads per version, needed for recency. The caller supplies a new directory.
func Write(ctx context.Context, c synthetic.Config, dir string, provenance map[string]string) (Manifest, error) {
	return WriteWithOptions(ctx, c, dir, provenance, Options{})
}

func WriteWithOptions(ctx context.Context, c synthetic.Config, dir string, provenance map[string]string, options Options) (Manifest, error) {
	g, err := synthetic.New(c)
	if err != nil {
		return Manifest{}, err
	}
	options, err = options.normalized(c.Start, time.Duration(c.SnapshotSeconds)*time.Second)
	if err != nil {
		return Manifest{}, err
	}
	if !options.Periods.Train.Start.Equal(c.Start) || !options.Periods.Test.End.Equal(c.End()) {
		return Manifest{}, fmt.Errorf("synthetic periods must cover the generator interval")
	}
	configJSON, err := json.Marshal(c)
	if err != nil {
		return Manifest{}, err
	}
	configHash := sha256.Sum256(configJSON)
	m := Manifest{Config: c, GeneratorVersion: synthetic.GeneratorVersion, ConfigSHA256: hex.EncodeToString(configHash[:]), Provenance: provenance}
	return writeStream(ctx, dir, g.Catalog(), func() (model.AccessEvent, bool, error) { e, ok := g.Next(); return e, ok, nil }, options, m)
}

// writeStream retains a bounded history window and one snapshot batch, never
// the whole object x snapshot matrix (20M rows in the acceptance profile).
func writeStream(ctx context.Context, dir string, catalog []model.Version, nextEvent func() (model.AccessEvent, bool, error), o Options, m Manifest) (Manifest, error) {
	m.FeatureRulesVersion = model.FeatureRulesVersion
	m.HorizonSeconds = int64(Horizon / time.Second)
	m.Train, m.Validation, m.Test = o.Periods.Train, o.Periods.Validation, o.Periods.Test
	m.Format = o.Format
	m.SnapshotSeconds = o.SnapshotEvery.Seconds()
	m.TrainEverySeconds = o.TrainEvery.Seconds()
	m.SplitFractions = o.Periods.Fractions()
	if m.GeneratorVersion == synthetic.GeneratorVersion {
		m.CoveragePolicy = "complete synthetic history by construction"
	} else {
		m.CoveragePolicy = "caller asserts complete selected input over explicit [start,end); not inferred from last event; source exclusions and sampling remain in source_manifest"
	}
	m.Rows, m.Eligible, m.SHA256 = map[string]int64{}, map[string]int64{}, map[string]string{}
	if o.Format == "csv.gz" {
		m.CompactColumns = CompactColumns()
	}
	if err := writeJSON(filepath.Join(dir, "catalog.json"), catalog); err != nil {
		return m, err
	}
	events, err := openLines(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		return m, err
	}
	defer events.f.Close()
	rows, err := openRows(dir, o.Format)
	if err != nil {
		return m, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = rows.Close()
		}
	}()
	var history []model.AccessEvent
	next, ok, err := nextEvent()
	if err != nil {
		return m, err
	}
	for at := m.Train.Start; at.Before(m.Test.End); at = at.Add(o.SnapshotEvery) {
		if err := ctx.Err(); err != nil {
			return m, err
		}
		end := at.Add(Horizon)
		for ok && next.OccurredAt.Before(end) {
			if err := ctx.Err(); err != nil {
				return m, err
			}
			if err := events.encoder.Encode(next); err != nil {
				return m, err
			}
			history = append(history, next)
			m.Events++
			next, ok, err = nextEvent()
			if err != nil {
				return m, err
			}
		}
		if o.TrainEvery == 0 || o.Periods.Split(at) != "train" || at.Sub(m.Train.Start)%o.TrainEvery == 0 {
			batch, err := rowsWithPeriods(ctx, model.FeatureRequest{At: at, Catalog: catalog, Events: history, Coverage: model.Coverage{Known: true, Start: m.Train.Start, Watermark: at}}, model.Coverage{Known: true, Start: m.Train.Start, Watermark: m.Test.End}, o.Periods)
			if err != nil {
				return m, err
			}
			for _, r := range batch {
				if err := rows.Write(r); err != nil {
					return m, err
				}
				m.Rows[r.Split]++
				if r.Eligible {
					m.Eligible[r.Split]++
				}
			}
		}
		history = trim(history, at.Add(o.SnapshotEvery).Add(-features.MaxWindow))
	}
	if ok {
		return m, fmt.Errorf("unconsumed event beyond dataset period")
	}
	if err := events.close(); err != nil {
		return m, err
	}
	err = rows.Close()
	closed = true
	if err != nil {
		return m, err
	}
	for _, name := range append([]string{"catalog.json", "events.jsonl"}, rows.Names()...) {
		hash, err := HashFile(filepath.Join(dir, name))
		if err != nil {
			return m, err
		}
		m.SHA256[name] = hash
	}
	return m, writeJSON(filepath.Join(dir, "manifest.json"), m)
}

func HashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

func trim(events []model.AccessEvent, before time.Time) []model.AccessEvent {
	i := sort.Search(len(events), func(i int) bool { return !events[i].OccurredAt.Before(before) })
	if i == 0 {
		return events
	}
	older := make(map[model.VersionRef][2]model.AccessEvent)
	for _, e := range events[:i] {
		if !e.IsDemand() {
			continue
		}
		key := model.VersionRef{Bucket: e.Bucket, ObjectKey: e.ObjectKey, VersionID: e.VersionID}
		pair := older[key]
		pair[0], pair[1] = pair[1], e
		older[key] = pair
	}
	kept := make([]model.AccessEvent, 0, len(events)-i+2*len(older))
	for _, pair := range older {
		for _, e := range pair {
			if !e.OccurredAt.IsZero() {
				kept = append(kept, e)
			}
		}
	}
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].OccurredAt.Equal(kept[j].OccurredAt) {
			return kept[i].Sequence < kept[j].Sequence
		}
		return kept[i].OccurredAt.Before(kept[j].OccurredAt)
	})
	kept = append(kept, events[i:]...)
	return kept
}

func Describe(m Manifest) string {
	return fmt.Sprintf("events=%d rows=%v eligible=%v", m.Events, m.Rows, m.Eligible)
}
