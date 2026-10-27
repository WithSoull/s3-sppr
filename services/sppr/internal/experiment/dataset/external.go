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
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/validator"
)

// WriteExternal treats explicitly supplied bounds as the caller's assertion of
// coverage for the selected trace. It never infers completeness from the last GET.
func WriteExternal(ctx context.Context, catalogPath, eventsPath, sourceManifestPath, dir string, provenance map[string]string, o Options) (Manifest, error) {
	o, err := o.normalized(o.Periods.Train.Start, 5*time.Minute)
	if err != nil {
		return Manifest{}, err
	}
	if o.Periods.Train.Start.IsZero() {
		return Manifest{}, fmt.Errorf("external start/end bounds are required")
	}
	f, err := os.Open(catalogPath)
	if err != nil {
		return Manifest{}, err
	}
	h := sha256.New()
	d := json.NewDecoder(io.TeeReader(f, h))
	d.DisallowUnknownFields()
	var catalog []model.Version
	err = d.Decode(&catalog)
	if err == nil {
		if d.Decode(new(any)) != io.EOF {
			err = fmt.Errorf("catalog must contain one JSON array")
		}
	}
	_ = f.Close()
	if err != nil {
		return Manifest{}, err
	}
	if len(catalog) == 0 {
		return Manifest{}, fmt.Errorf("empty catalog")
	}
	if _, err := features.NewService().Build(ctx, model.FeatureRequest{At: o.Periods.Train.Start, Catalog: catalog}); err != nil {
		return Manifest{}, err
	}
	versions := make(map[model.VersionRef]model.Version, len(catalog))
	for _, v := range catalog {
		versions[v.VersionRef] = v
	}
	m := Manifest{GeneratorVersion: "external-v1", Provenance: provenance, InputSHA256: map[string]string{"catalog.json": hex.EncodeToString(h.Sum(nil))}}
	if sourceManifestPath != "" {
		m.SourceManifest, err = os.ReadFile(sourceManifestPath)
		if err != nil {
			return m, err
		}
		if !json.Valid(m.SourceManifest) {
			return m, fmt.Errorf("invalid source manifest")
		}
		sum := sha256.Sum256(m.SourceManifest)
		m.InputSHA256["manifest.json"] = hex.EncodeToString(sum[:])
	}
	f, err = os.Open(eventsPath)
	if err != nil {
		return m, err
	}
	defer f.Close()
	h = sha256.New()
	d = json.NewDecoder(bufio.NewReaderSize(io.TeeReader(f, h), 256*1024))
	d.DisallowUnknownFields()
	last := time.Time{}
	sequences := map[[2]string]int64{}
	next := func() (model.AccessEvent, bool, error) {
		var e model.AccessEvent
		if err := d.Decode(&e); err != nil {
			if err == io.EOF {
				return e, false, nil
			}
			return e, false, fmt.Errorf("decode event: %w", err)
		}
		if err := validator.AccessEvent(e); err != nil {
			return e, false, err
		}
		if e.OccurredAt.Before(last) || e.OccurredAt.Before(o.Periods.Train.Start) || !e.OccurredAt.Before(o.Periods.Test.End) {
			return e, false, fmt.Errorf("events must be sorted and inside [start,end)")
		}
		stream := [2]string{e.GatewayID, e.StreamID}
		if e.Sequence <= sequences[stream] {
			return e, false, fmt.Errorf("duplicate or non-monotonic sequence")
		}
		sequences[stream] = e.Sequence
		last = e.OccurredAt
		v, ok := versions[model.VersionRef{Bucket: e.Bucket, ObjectKey: e.ObjectKey, VersionID: e.VersionID}]
		if !ok || !v.Active(e.OccurredAt) || e.Bytes > v.SizeBytes {
			return e, false, fmt.Errorf("event outside catalogue identity, lifetime or size")
		}
		return e, true, nil
	}
	m, err = writeStream(ctx, dir, catalog, next, o, m)
	if err != nil {
		return m, err
	}
	m.InputSHA256["events.jsonl"] = hex.EncodeToString(h.Sum(nil))
	if len(m.SourceManifest) > 0 {
		var source struct {
			SHA256 map[string]string `json:"sha256"`
		}
		if err := json.Unmarshal(m.SourceManifest, &source); err != nil {
			return m, err
		}
		for _, name := range []string{"catalog.json", "events.jsonl"} {
			if want, ok := source.SHA256[name]; ok && want != m.InputSHA256[name] {
				return m, fmt.Errorf("source manifest hash mismatch for %s", name)
			}
		}
	}
	return m, writeJSON(filepath.Join(dir, "manifest.json"), m)
}
