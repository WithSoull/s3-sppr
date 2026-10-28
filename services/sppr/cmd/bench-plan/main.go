// Command bench-plan measures in-memory production features, forecasts and planner.Build.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/planner"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
)

type measurement struct {
	Method       string            `json:"method"`
	Model        string            `json:"model_version,omitempty"`
	Seconds      float64           `json:"wall_seconds"`
	HeapBase     uint64            `json:"go_heap_base_bytes"`
	HeapPeak     uint64            `json:"go_heap_peak_bytes"`
	HeapIncrease uint64            `json:"go_heap_peak_increase_bytes"`
	MaxRSS       uint64            `json:"process_max_rss_bytes"`
	Rows         int               `json:"feature_rows"`
	Forecasts    int               `json:"forecasts"`
	Summary      model.PlanSummary `json:"summary"`
}
type report struct {
	Schema         string           `json:"schema"`
	Generator      synthetic.Config `json:"generator"`
	GoVersion      string           `json:"go_version"`
	OS             string           `json:"os"`
	Arch           string           `json:"arch"`
	GOMAXPROCS     int              `json:"gomaxprocs"`
	HistoryRows    int              `json:"minute_history_rows"`
	HorizonSeconds int64            `json:"horizon_seconds"`
	Scope          string           `json:"scope"`
	Memory         string           `json:"memory_measurement"`
	Cycles         []measurement    `json:"cycles"`
}

// prepare reuses synthetic-v1 and folds events into the same minute input as
// the live planner. Generation and aggregation are outside the timed cycle.
func prepare(ctx context.Context, cfg synthetic.Config) (model.FeatureRequest, error) {
	g, err := synthetic.New(cfg)
	if err != nil {
		return model.FeatureRequest{}, err
	}
	at := cfg.Start.Add(24 * time.Hour)
	req := model.FeatureRequest{At: at, Catalog: g.Catalog(), Coverage: model.Coverage{Known: true, Start: cfg.Start, Watermark: at}}
	minute := cfg.Start
	counts := map[model.VersionRef]model.MinuteReads{}
	flush := func() {
		rows := make([]model.MinuteReads, 0, len(counts))
		for _, r := range counts {
			rows = append(rows, r)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].VersionID < rows[j].VersionID })
		req.Reads = append(req.Reads, rows...)
		clear(counts)
	}
	for {
		if err := ctx.Err(); err != nil {
			return req, err
		}
		e, ok := g.Next()
		if !ok || !e.OccurredAt.Before(at) {
			break
		}
		m := e.OccurredAt.Truncate(time.Minute)
		if m != minute {
			flush()
			minute = m
		}
		ref := model.VersionRef{Bucket: e.Bucket, ObjectKey: e.ObjectKey, VersionID: e.VersionID}
		r := counts[ref]
		r.VersionRef, r.Minute = ref, m
		r.Reads++
		r.Bytes += e.Bytes
		counts[ref] = r
	}
	flush()
	return req, nil
}
func heapBytes() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}
func maxRSSBytes() (uint64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	rss := uint64(usage.Maxrss)
	if runtime.GOOS != "darwin" {
		rss *= 1024
	}
	return rss, nil
}
func cycle(ctx context.Context, req model.FeatureRequest, predictor service.Predictor, horizon time.Duration) (measurement, error) {
	runtime.GC()
	r := measurement{Method: predictor.MethodVersion(), Model: predictor.ModelVersion(), HeapBase: heapBytes()}
	var peak atomic.Uint64
	peak.Store(r.HeapBase)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			peak.Store(max(peak.Load(), heapBytes()))
			select {
			case <-stop:
				peak.Store(max(peak.Load(), heapBytes()))
				return
			case <-ticker.C:
			}
		}
	}()
	started := time.Now()
	result, rows, forecasts, err := build(ctx, req, predictor, horizon)
	r.Seconds = time.Since(started).Seconds()
	// Keep the result alive through the final heap sample, even for a short cycle.
	close(stop)
	<-done
	runtime.KeepAlive(result)
	r.HeapPeak = peak.Load()
	r.HeapIncrease = r.HeapPeak - r.HeapBase
	r.Rows, r.Forecasts, r.Summary = rows, forecasts, result.Summary
	if err != nil {
		return r, err
	}
	r.MaxRSS, err = maxRSSBytes()
	return r, err
}
func build(ctx context.Context, req model.FeatureRequest, predictor service.Predictor, horizon time.Duration) (model.PlanResult, int, int, error) {
	rows, err := features.NewService().Build(ctx, req)
	if err != nil {
		return model.PlanResult{}, 0, 0, err
	}
	n := len(rows)
	hotCount := int(math.Floor(.0921 * float64(n)))
	size := req.Catalog[0].SizeBytes
	in := model.PlanInput{At: req.At, MethodVersion: predictor.MethodVersion(), BudgetBytes: 256 * model.MiB,
		Params: model.PlannerParams{Horizon: horizon, HotMiBPerMs: .1, ColdMiBPerMs: .025, HotUtilization: .9, ColdUtilization: .9, MaxRejectedSaved: 1000},
		Hot:    model.ClassState{CapacityBytes: max(size, int64(math.Ceil(.1024*float64(n)))*size), MaxUtilization: .9, UsedBytes: int64(hotCount) * size},
		Cold:   model.ClassState{CapacityBytes: int64(math.Ceil(1.2288*float64(n))) * size, MaxUtilization: .9, UsedBytes: int64(n-hotCount) * size},
	}
	forecasts := 0
	for i, fs := range rows {
		if err := ctx.Err(); err != nil {
			return model.PlanResult{}, n, forecasts, err
		}
		if !fs.Quality.Usable {
			return model.PlanResult{}, n, forecasts, fmt.Errorf("unusable feature row %s: %s", fs.VersionID, fs.Quality.Reason)
		}
		forecast, err := predictor.Predict(fs, horizon)
		if err != nil {
			return model.PlanResult{}, n, forecasts, fmt.Errorf("forecast %s: %w", fs.VersionID, err)
		}
		forecasts++
		class := model.StorageClassCold
		// Spread the initial HOT set through the catalogue independently of popularity.
		if hotCount > 0 && i*hotCount/n != (i+1)*hotCount/n {
			class = model.StorageClassHot
		}
		p := model.Placement{VersionID: fs.VersionID, Bucket: fs.Bucket, Key: fs.ObjectKey, SizeBytes: fs.SizeBytes,
			Class: class, IsCurrent: true, Generation: 1, CreatedAt: req.Catalog[i].CreatedAt, UpdatedAt: req.Catalog[i].CreatedAt}
		f := fs.Features
		fraction := 1.0
		if f.Reads24h > 0 {
			fraction = f.MeanReadFraction
		}
		in.Items = append(in.Items, model.PlanItem{Placement: p, Features: &f, Forecast: &forecast, ReadFraction: fraction})
	}
	id := 0
	result, err := planner.Build(in, func() string { id++; return fmt.Sprintf("bench-%d", id) })
	return result, n, forecasts, err
}
func run(args []string) error {
	f := flag.NewFlagSet("bench-plan", flag.ContinueOnError)
	config := f.String("config", "services/sppr/examples/data/acceptance.json", "synthetic-v1 config; history covers first 24h")
	n := f.Int("objects", 10000, "catalogue size (10..100000)")
	modelDir := f.String("model", "", "optional exported LGBM model directory; also runs EWMA")
	horizon := f.Duration("horizon", time.Hour, "forecast horizon; must match model manifest")
	out := f.String("out", "", "new JSON output file; default stdout")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *horizon <= 0 || *horizon%time.Second != 0 {
		return fmt.Errorf("unexpected arguments or invalid horizon")
	}
	if *out != "" {
		if _, err := os.Stat(*out); !os.IsNotExist(err) {
			return fmt.Errorf("output exists or cannot be inspected: %s", *out)
		}
	}
	raw, err := os.ReadFile(*config)
	if err != nil {
		return err
	}
	var cfg synthetic.Config
	if err = json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	cfg.Objects = *n
	// A fixed catalogue is needed to compare both cycles on exactly the same input.
	if cfg.Lifecycle {
		return fmt.Errorf("bench-plan requires lifecycle=false")
	}
	if err = cfg.Validate(); err != nil {
		return err
	}
	predictors := []service.Predictor{ewma.NewPredictor()}
	if *modelDir != "" {
		m, err := lgbm.Load(*modelDir, *horizon)
		if err != nil {
			return err
		}
		predictors = append(predictors, m)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	req, err := prepare(ctx, cfg)
	if err != nil {
		return err
	}
	r := report{Schema: "bench-plan-v1", Generator: cfg, GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), HistoryRows: len(req.Reads), HorizonSeconds: int64(*horizon / time.Second),
		Scope:  "features.Build + per-object production predictor + planner.Build; inputs in memory; excludes generation, model load, DB/RPC and persistence; budget 256 MiB; HOT/COLD capacities scale with N; HOT/COLD full MiB read 10/40 ms",
		Memory: "Go heap objects sampled every 1ms plus boundary samples (lower bound); includes resident inputs/models; RSS is process lifetime high-water mark, cumulative across cycles; not cgroup memory"}
	for _, p := range predictors {
		m, err := cycle(ctx, req, p, *horizon)
		if err != nil {
			return err
		}
		r.Cycles = append(r.Cycles, m)
	}
	var file *os.File
	if *out == "" {
		file = os.Stdout
	} else {
		if err = os.MkdirAll(filepath.Dir(*out), 0755); err != nil {
			return err
		}
		file, err = os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return err
		}
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	err = enc.Encode(r)
	if file != os.Stdout {
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
	}
	return err
}
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
