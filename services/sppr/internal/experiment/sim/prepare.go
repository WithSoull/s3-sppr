package sim

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/baseline"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
)

type minute struct {
	Object int
	Count  int64
}
type Demand struct {
	Catalog []model.Version
	Reads   []Read
	Minutes [][]minute
}
type Training struct {
	Cutoff     int64   `json:"cutoff_ns"`
	Activation int64   `json:"activation_ns"`
	Seconds    float64 `json:"seconds"`
	Version    string  `json:"version"`
	Dir        string  `json:"dir"`
}
type CacheManifest struct {
	Version  string            `json:"version"`
	Config   Config            `json:"config"`
	SHA256   map[string]string `json:"sha256"`
	Training []Training        `json:"training"`
	Seconds  float64           `json:"seconds"`
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

func Generate(ctx context.Context, c Config) (Demand, error) {
	g, err := synthetic.New(c.Generator)
	if err != nil {
		return Demand{}, err
	}
	d := Demand{Catalog: g.Catalog(), Minutes: make([][]minute, 7*24*60)}
	idx := map[string]int{}
	for i := range d.Catalog {
		idx[d.Catalog[i].VersionID] = i
		if len(c.Sizes) > 0 {
			d.Catalog[i].SizeBytes = c.Sizes[i%len(c.Sizes)]
		}
	}
	counts := make([]int64, len(d.Catalog))
	lastMinute := 0
	flush := func(m int) {
		for i, n := range counts {
			if n > 0 {
				d.Minutes[m] = append(d.Minutes[m], minute{i, n})
				counts[i] = 0
			}
		}
	}
	for {
		e, ok := g.Next()
		if !ok {
			break
		}
		if e.Sequence%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return d, err
			}
		}
		at := int64(e.OccurredAt.Sub(c.Generator.Start))
		m := int(at / int64(time.Minute))
		if m != lastMinute {
			flush(lastMinute)
			lastMinute = m
		}
		i := idx[e.VersionID]
		counts[i]++
		if at >= int64(72*time.Hour) {
			d.Reads = append(d.Reads, Read{at, i, max(1, int64(float64(d.Catalog[i].SizeBytes)*c.ReadFraction))})
		}
	}
	flush(lastMinute)
	return d, nil
}
func (d Demand) Snapshots(ctx context.Context, c Config, at time.Duration) ([]model.FeatureSnapshot, error) {
	start := max(0, int(at/time.Minute)-24*60)
	end := int(at / time.Minute)
	n := 0
	for _, ms := range d.Minutes[start:end] {
		n += len(ms)
	}
	reads := make([]model.MinuteReads, 0, n)
	for m := start; m < end; m++ {
		for _, r := range d.Minutes[m] {
			v := d.Catalog[r.Object]
			bytes := max(1, int64(float64(v.SizeBytes)*c.ReadFraction))
			reads = append(reads, model.MinuteReads{VersionRef: v.VersionRef, Minute: c.Generator.Start.Add(time.Duration(m) * time.Minute), Reads: r.Count, Bytes: r.Count * bytes})
		}
	}
	return features.NewService().Build(ctx, model.FeatureRequest{At: c.Generator.Start.Add(at), Catalog: d.Catalog, Reads: reads, Coverage: model.Coverage{Known: true, Start: c.Generator.Start, Watermark: c.Generator.Start.Add(at)}})
}
func (d Demand) Labels(at time.Duration) []int64 {
	out := make([]int64, len(d.Catalog))
	for m := int(at / time.Minute); m < min(len(d.Minutes), int((at+time.Hour)/time.Minute)); m++ {
		for _, r := range d.Minutes[m] {
			out[r.Object] += r.Count
		}
	}
	return out
}

type trainingWriter struct {
	cutoff   time.Duration
	dir      string
	writer   dataset.RowWriter
	manifest dataset.Manifest
}

func Prepare(ctx context.Context, c Config, dir string) (CacheManifest, error) {
	started := time.Now()
	manifest := CacheManifest{Version: Version, Config: c, SHA256: map[string]string{}}
	if err := c.Validate(); err != nil {
		return manifest, err
	}
	if err := os.Mkdir(dir, 0755); err != nil {
		return manifest, err
	}
	fmt.Printf("%s/%d: generating demand\n", c.Generator.Scenario, c.Generator.Seed)
	d, err := Generate(ctx, c)
	if err != nil {
		return manifest, err
	}
	if err := writeGob(filepath.Join(dir, "demand.gob"), d); err != nil {
		return manifest, err
	}
	var writers []trainingWriter
	cutoffs := []time.Duration{48 * time.Hour}
	if c.UpdateModel {
		for h := 72; h <= 144; h += 24 {
			cutoffs = append(cutoffs, time.Duration(h)*time.Hour)
		}
	}
	defer func() {
		for _, w := range writers {
			_ = w.writer.Close()
		}
	}()
	for _, cut := range cutoffs {
		sub := filepath.Join(dir, fmt.Sprintf("train-%03d", int(cut/time.Hour)))
		if err := os.Mkdir(sub, 0755); err != nil {
			return manifest, err
		}
		w, err := dataset.OpenRowWriter(sub, "csv.gz")
		if err != nil {
			return manifest, err
		}
		start := c.Generator.Start.Add(cut - 48*time.Hour)
		end := c.Generator.Start.Add(cut)
		writers = append(writers, trainingWriter{cut, sub, w, dataset.Manifest{Config: c.Generator, Format: "csv.gz", GeneratorVersion: synthetic.GeneratorVersion, FeatureRulesVersion: model.FeatureRulesVersion, HorizonSeconds: 3600, Train: model.Interval{Start: start, End: end}, Validation: model.Interval{Start: end, End: end}, Test: model.Interval{Start: end, End: end}, SHA256: map[string]string{}, Provenance: map[string]string{"labels": "minute aggregates, [T,T+1h); available no later than cutoff; rolling 48h train"}}})
	}
	featFile, err := os.Create(filepath.Join(dir, "features.gob"))
	if err != nil {
		return manifest, err
	}
	buf := bufio.NewWriterSize(featFile, 1<<20)
	enc := gob.NewEncoder(buf)
	var ages, recencies []baseline.Sample
	fmt.Printf("%s/%d: building shared features\n", c.Generator.Scenario, c.Generator.Seed)
	for at := time.Duration(0); at < 168*time.Hour; at += time.Duration(c.CycleSeconds) * time.Second {
		training := at%(15*time.Minute) == 0 && at+time.Hour <= cutoffs[len(cutoffs)-1]
		test := at >= 72*time.Hour
		if !training && !test {
			continue
		}
		rows, err := d.Snapshots(ctx, c, at)
		if err != nil {
			featFile.Close()
			return manifest, err
		}
		labels := d.Labels(at)
		if training {
			for _, w := range writers {
				if at < w.cutoff-48*time.Hour || at+time.Hour > w.cutoff {
					continue
				}
				for i, s := range rows {
					n := labels[i]
					has := n > 0
					f, err := ewma.Predict(s, time.Hour)
					if err != nil {
						return manifest, err
					}
					r := dataset.Row{Snapshot: s, Forecast: f, Split: "train", Label: dataset.Label{Reads: &n, HasRead: &has, AvailableAt: s.At.Add(time.Hour), Quality: model.DataQuality{Usable: true, Reason: "complete"}}, Eligible: s.Quality.Usable}
					if err := w.writer.Write(r); err != nil {
						return manifest, err
					}
				}
			}
			if at+time.Hour <= 48*time.Hour {
				for i, s := range rows {
					if !s.Quality.Usable {
						continue
					}
					ages = append(ages, baseline.Sample{X: s.Features.AgeSeconds, Reads: float64(labels[i])})
					x := math.NaN()
					if s.Features.LastReadSeconds != nil {
						x = *s.Features.LastReadSeconds
					}
					recencies = append(recencies, baseline.Sample{X: x, Reads: float64(labels[i])})
				}
			}
		}
		if test {
			f := Frame{At: int64(at), Features: make([]model.Features, len(rows)), Labels: labels}
			for i, s := range rows {
				f.Features[i] = s.Features
			}
			if err := enc.Encode(f); err != nil {
				return manifest, err
			}
		}
		if at%(24*time.Hour) == 0 {
			fmt.Printf("%s/%d: features at %dh\n", c.Generator.Scenario, c.Generator.Seed, int(at/time.Hour))
		}
	}
	if err := buf.Flush(); err != nil {
		return manifest, err
	}
	if err := featFile.Close(); err != nil {
		return manifest, err
	}
	for _, fit := range []struct {
		name, feature string
		samples       []baseline.Sample
	}{{"age", "age_seconds", ages}, {"recency", "last_read_seconds", recencies}} {
		m, err := baseline.Fit(fit.feature, fit.samples)
		if err != nil {
			return manifest, err
		}
		if err := m.Save(filepath.Join(dir, fit.name+"-baseline-map.json")); err != nil {
			return manifest, err
		}
	}
	// Both the samples and minute histories may be released before Python fits.
	ages, recencies = nil, nil
	d.Minutes = nil
	for _, w := range writers {
		if err := w.writer.Close(); err != nil {
			return manifest, err
		}
		for _, name := range w.writer.Names() {
			h, err := dataset.HashFile(filepath.Join(w.dir, name))
			if err != nil {
				return manifest, err
			}
			w.manifest.SHA256[name] = h
		}
		if err := writeJSON(filepath.Join(w.dir, "manifest.json"), w.manifest); err != nil {
			return manifest, err
		}
		if !slices.Contains(c.Policies, "lgbm") {
			continue
		}
		out := filepath.Join(dir, fmt.Sprintf("model-%03d", int(w.cutoff/time.Hour)))
		t := time.Now()
		cmd := exec.CommandContext(ctx, c.Python, "-m", "sppr_ml", "train", "--data", w.dir, "--out", out, "--params", string(c.Params), "--fit-only")
		cmd.Env = append(os.Environ(), "PYTHONPATH="+c.MLDir, "OMP_NUM_THREADS=2", "OPENBLAS_NUM_THREADS=2", "MPLCONFIGDIR="+filepath.Join(dir, "mpl"))
		log, err := os.Create(filepath.Join(dir, fmt.Sprintf("train-%03d.log", int(w.cutoff/time.Hour))))
		if err != nil {
			return manifest, err
		}
		cmd.Stdout, cmd.Stderr = log, log
		err = cmd.Run()
		log.Close()
		if err != nil {
			return manifest, fmt.Errorf("training (see %s): %w", log.Name(), err)
		}
		m, err := lgbm.Load(out, time.Hour)
		if err != nil {
			return manifest, err
		}
		elapsed := time.Since(t)
		manifest.Training = append(manifest.Training, Training{Cutoff: int64(w.cutoff), Activation: int64(w.cutoff + elapsed), Seconds: elapsed.Seconds(), Version: m.ModelVersion(), Dir: filepath.Base(out)})
	}
	writers = nil
	if err := Forecast(ctx, c, dir, d.Catalog, manifest.Training); err != nil {
		return manifest, err
	}
	for _, name := range []string{"demand.gob", "features.gob", "forecasts.gob", "age-baseline-map.json", "recency-baseline-map.json"} {
		h, err := dataset.HashFile(filepath.Join(dir, name))
		if err != nil {
			return manifest, err
		}
		manifest.SHA256[name] = h
	}
	manifest.Seconds = time.Since(started).Seconds()
	return manifest, writeJSON(filepath.Join(dir, "manifest.json"), manifest)
}
func writeGob(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	b := bufio.NewWriterSize(f, 1<<20)
	err = gob.NewEncoder(b).Encode(v)
	if err == nil {
		err = b.Flush()
	}
	ce := f.Close()
	if err != nil {
		return err
	}
	return ce
}
func readGob(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return gob.NewDecoder(bufio.NewReaderSize(f, 1<<20)).Decode(v)
}
func Forecast(ctx context.Context, c Config, dir string, catalog []model.Version, training []Training) error {
	predictors := map[string]service.Predictor{"ewma": ewma.NewPredictor()}
	for _, kind := range []string{"age", "recency", "frequency"} {
		var m baseline.Map
		var err error
		if kind != "frequency" {
			m, err = baseline.Load(filepath.Join(dir, kind+"-baseline-map.json"))
			if err != nil {
				return err
			}
		}
		predictors[kind], err = baseline.New(kind, m)
		if err != nil {
			return err
		}
	}
	models := []service.Predictor{}
	for _, t := range training {
		m, err := lgbm.Load(filepath.Join(dir, t.Dir), time.Hour)
		if err != nil {
			return err
		}
		models = append(models, m)
	}
	in, err := os.Open(filepath.Join(dir, "features.gob"))
	if err != nil {
		return err
	}
	defer in.Close()
	dec := gob.NewDecoder(bufio.NewReaderSize(in, 1<<20))
	out, err := os.Create(filepath.Join(dir, "forecasts.gob"))
	if err != nil {
		return err
	}
	defer out.Close()
	buf := bufio.NewWriterSize(out, 1<<20)
	enc := gob.NewEncoder(buf)
	for {
		var f Frame
		if err := dec.Decode(&f); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		for i, t := range training {
			if t.Activation <= f.At {
				predictors["lgbm"] = models[i]
			}
		}
		f.Forecasts = map[string][]float64{}
		f.ModelVersions = map[string]string{}
		for _, kind := range c.Policies {
			p := predictors[kind]
			if p == nil {
				return fmt.Errorf("no model active at logical time %s", time.Duration(f.At))
			}
			values := make([]float64, len(catalog))
			for i, v := range catalog {
				s := model.FeatureSnapshot{VersionRef: v.VersionRef, At: c.Generator.Start.Add(time.Duration(f.At)), SizeBytes: v.SizeBytes, Features: f.Features[i], RulesVersion: model.FeatureRulesVersion, Quality: model.DataQuality{Usable: true, Reason: "complete"}}
				pred, err := p.Predict(s, time.Hour)
				if err != nil {
					return err
				}
				values[i] = pred.ExpectedReads
			}
			f.Forecasts[kind] = values
			f.ModelVersions[kind] = p.ModelVersion()
		}
		if err := enc.Encode(f); err != nil {
			return err
		}
	}
	return buf.Flush()
}
func jsonEqual(a, b json.RawMessage) bool {
	var x, y bytes.Buffer
	if json.Compact(&x, a) != nil || json.Compact(&y, b) != nil {
		return false
	}
	return bytes.Equal(x.Bytes(), y.Bytes())
}
func Replay(ctx context.Context, c Config, dir string) ([]Result, error) {
	var manifest CacheManifest
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &manifest); err != nil {
		return nil, err
	}
	if manifest.Version != Version || !reflect.DeepEqual(c.Generator, manifest.Config.Generator) || !reflect.DeepEqual(c.Sizes, manifest.Config.Sizes) || c.ReadFraction != manifest.Config.ReadFraction || c.UpdateModel != manifest.Config.UpdateModel || c.CycleSeconds != manifest.Config.CycleSeconds || !jsonEqual(c.Params, manifest.Config.Params) {
		return nil, fmt.Errorf("incompatible cache configuration")
	}
	for _, name := range []string{"demand.gob", "features.gob", "forecasts.gob", "age-baseline-map.json", "recency-baseline-map.json"} {
		h, err := dataset.HashFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		if manifest.SHA256[name] != h {
			return nil, fmt.Errorf("cache hash differs: %s", name)
		}
	}

	var d Demand
	if err := readGob(filepath.Join(dir, "demand.gob"), &d); err != nil {
		return nil, err
	}
	d.Minutes = nil
	var engines []*Engine
	for _, p := range c.Policies {
		for _, b := range c.Budgets {
			e, err := NewEngine(c, d.Catalog, d.Reads, p, b)
			if err != nil {
				return nil, err
			}
			engines = append(engines, e)
		}
	}
	f, err := os.Open(filepath.Join(dir, "forecasts.gob"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := gob.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	for {
		var frame Frame
		if err := dec.Decode(&frame); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, e := range engines {
			if err := e.Plan(frame); err != nil {
				return nil, err
			}
			if frame.At+int64(time.Hour) <= int64(168*time.Hour) {
				for i, n := range frame.Labels {
					e.mae += math.Abs(frame.Forecasts[e.result.Policy][i] - float64(n))
					e.predictions++
				}
			}
		}
	}
	results := make([]Result, 0, len(engines))
	for _, e := range engines {
		r, err := e.Finish()
		if err != nil {
			return nil, err
		}
		if e.predictions > 0 {
			r.ForecastMAE = e.mae / float64(e.predictions)
		}
		if r.Policy == "lgbm" {
			r.Models = manifest.Training
		}
		results = append(results, r)
	}
	return results, nil
}
func WriteResults(dir string, results []Result, gitSHA string) error {
	if err := os.Mkdir(dir, 0755); err != nil {
		return err
	}
	for _, r := range results {
		r.Versions["git_sha"] = gitSHA
		stem := fmt.Sprintf("%s-%d", r.Policy, r.Budget)
		if err := writeJSON(filepath.Join(dir, stem+".json"), r); err != nil {
			return err
		}
		if r.Config.RawLatencies {
			f, err := os.Create(filepath.Join(dir, stem+"-latencies.json.gz"))
			if err != nil {
				return err
			}
			gz := gzip.NewWriter(f)
			err = json.NewEncoder(gz).Encode(r.raw)
			ce := gz.Close()
			fe := f.Close()
			if err != nil {
				return err
			}
			if ce != nil {
				return ce
			}
			if fe != nil {
				return fe
			}
		}
	}
	return nil
}
