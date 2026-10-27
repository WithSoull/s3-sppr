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
	Config              synthetic.Config  `json:"config"`
	GeneratorVersion    string            `json:"generator_version"`
	FeatureRulesVersion string            `json:"feature_rules_version"`
	ConfigSHA256        string            `json:"config_sha256"`
	HorizonSeconds      int64             `json:"horizon_seconds"`
	Train               model.Interval    `json:"train"`
	Validation          model.Interval    `json:"validation"`
	Test                model.Interval    `json:"test"`
	Events              int64             `json:"events"`
	Rows                map[string]int64  `json:"rows"`
	Eligible            map[string]int64  `json:"eligible"`
	SHA256              map[string]string `json:"sha256"`
	Provenance          map[string]string `json:"provenance"`
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
	g, err := synthetic.New(c)
	if err != nil {
		return Manifest{}, err
	}
	catalog := g.Catalog()
	configJSON, err := json.Marshal(c)
	if err != nil {
		return Manifest{}, err
	}
	configHash := sha256.Sum256(configJSON)
	m := Manifest{Config: c, GeneratorVersion: synthetic.GeneratorVersion, FeatureRulesVersion: model.FeatureRulesVersion, HorizonSeconds: 3600,
		ConfigSHA256: hex.EncodeToString(configHash[:]),
		Train:        model.Interval{Start: c.Start, End: c.Start.Add(48 * time.Hour)}, Validation: model.Interval{Start: c.Start.Add(48 * time.Hour), End: c.Start.Add(72 * time.Hour)}, Test: model.Interval{Start: c.Start.Add(72 * time.Hour), End: c.End()}, Rows: map[string]int64{}, Eligible: map[string]int64{}, SHA256: map[string]string{}, Provenance: provenance}
	if err := writeJSON(filepath.Join(dir, "catalog.json"), catalog); err != nil {
		return m, err
	}
	events, err := openLines(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		return m, err
	}
	defer events.f.Close()
	rows, err := openLines(filepath.Join(dir, "slices.jsonl"))
	if err != nil {
		return m, err
	}
	defer rows.f.Close()
	var history []model.AccessEvent
	next, ok := g.Next()
	for at := c.Start; at.Before(c.End()); at = at.Add(time.Duration(c.SnapshotSeconds) * time.Second) {
		if err := ctx.Err(); err != nil {
			return m, err
		}
		end := at.Add(Horizon)
		for ok && next.OccurredAt.Before(end) {
			if err := events.encoder.Encode(next); err != nil {
				return m, err
			}
			history = append(history, next)
			m.Events++
			next, ok = g.Next()
		}
		batch, err := Rows(ctx, model.FeatureRequest{At: at, Catalog: catalog, Events: history, Coverage: model.Coverage{Known: true, Start: c.Start, Watermark: at}}, model.Coverage{Known: true, Start: c.Start, Watermark: c.End()}, c.Start)
		if err != nil {
			return m, err
		}
		for _, r := range batch {
			if err := rows.encoder.Encode(r); err != nil {
				return m, err
			}
			m.Rows[r.Split]++
			if r.Eligible {
				m.Eligible[r.Split]++
			}
		}
		history = trim(history, at.Add(time.Duration(c.SnapshotSeconds)*time.Second).Add(-features.MaxWindow))
	}
	if err := events.close(); err != nil {
		return m, err
	}
	if err := rows.close(); err != nil {
		return m, err
	}
	for _, name := range []string{"catalog.json", "events.jsonl", "slices.jsonl"} {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return m, err
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return m, err
		}
		m.SHA256[name] = hex.EncodeToString(h.Sum(nil))
	}
	return m, writeJSON(filepath.Join(dir, "manifest.json"), m)
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
