package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error { return runArgs(os.Args[1:]) }

func runArgs(args []string) error {
	flag := flag.NewFlagSet("dataset", flag.ContinueOnError)
	demo := flag.Bool("demo", false, "print hand-checkable lesson rows to stdout")
	configPath := flag.String("config", "", "generator JSON configuration")
	out := flag.String("out", "", "new output directory; existing paths are refused")
	scenario := flag.String("scenario", "", "override scenario")
	seed := flag.Int64("seed", -1, "override seed (0..9 for acceptance runs)")
	catalogPath := flag.String("catalog", "", "external catalog.json (requires -events, -start, -end)")
	eventsPath := flag.String("events", "", "external sorted events.jsonl")
	sourceManifest := flag.String("source-manifest", "", "source manifest; defaults to catalog sibling manifest.json if present")
	start := flag.String("start", "", "external coverage start, RFC3339")
	end := flag.String("end", "", "external coverage end (exclusive), RFC3339")
	trainEnd := flag.String("train-end", "", "train end, RFC3339; required for non-seven-day traces")
	validationEnd := flag.String("validation-end", "", "validation end, RFC3339; required for non-seven-day traces")
	format := flag.String("format", "jsonl", "jsonl or csv.gz (one file per split, including purged)")
	snapshotEvery := flag.Duration("snapshot-every", 0, "snapshot interval; external default 5m, synthetic default from config")
	trainEvery := flag.Duration("train-every", 0, "thin train to this interval, e.g. 15m (0 keeps all)")
	if err := flag.Parse(args); err != nil {
		return err
	}
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	options := dataset.Options{Format: *format, SnapshotEvery: *snapshotEvery, TrainEvery: *trainEvery}
	external := *catalogPath != "" || *eventsPath != ""
	if external {
		if *catalogPath == "" || *eventsPath == "" || *configPath != "" || *scenario != "" || *seed >= 0 || *demo {
			return fmt.Errorf("use both -catalog and -events, without generator/demo flags")
		}
		from, err := time.Parse(time.RFC3339Nano, *start)
		if err != nil {
			return fmt.Errorf("-start: %w", err)
		}
		to, err := time.Parse(time.RFC3339Nano, *end)
		if err != nil {
			return fmt.Errorf("-end: %w", err)
		}
		options.Periods = dataset.DefaultPeriods(from)
		options.Periods.Test.End = to
		if *trainEnd != "" || *validationEnd != "" {
			t, err := time.Parse(time.RFC3339Nano, *trainEnd)
			if err != nil {
				return fmt.Errorf("-train-end: %w", err)
			}
			v, err := time.Parse(time.RFC3339Nano, *validationEnd)
			if err != nil {
				return fmt.Errorf("-validation-end: %w", err)
			}
			options.Periods.Train.End = t
			options.Periods.Validation.Start = t
			options.Periods.Validation.End = v
			options.Periods.Test.Start = v
		} else if to.Sub(from) != 7*24*time.Hour {
			return fmt.Errorf("non-seven-day input requires -train-end and -validation-end")
		}
		if err := options.Periods.Validate(); err != nil {
			return err
		}
		if *sourceManifest == "" {
			candidate := filepath.Join(filepath.Dir(*catalogPath), "manifest.json")
			if _, err := os.Stat(candidate); err == nil {
				*sourceManifest = candidate
			} else if !os.IsNotExist(err) {
				return err
			}
		}
	} else if *start != "" || *end != "" || *trainEnd != "" || *validationEnd != "" || *sourceManifest != "" {
		return fmt.Errorf("external bounds/manifest require -catalog and -events")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *demo {
		req := dataset.Lesson()
		rows, err := dataset.Rows(ctx, req, model.Coverage{Known: true, Start: req.Coverage.Start, Watermark: req.At.Add(dataset.Horizon)}, req.Coverage.Start)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(rows)
	}
	if *out == "" || (!external && *configPath == "") {
		return fmt.Errorf("use -demo, -config <json> -out <new-directory>, or -catalog/-events/-start/-end/-out")
	}
	var cfg synthetic.Config
	if !external {
		f, err := os.Open(*configPath)
		if err != nil {
			return err
		}
		defer f.Close()
		decoder := json.NewDecoder(f)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cfg); err != nil {
			return err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return fmt.Errorf("configuration must contain one JSON object")
		}
		if *scenario != "" {
			cfg.Scenario = *scenario
		}
		if *seed >= 0 {
			cfg.Seed = uint64(*seed)
		}
		if err := cfg.Validate(); err != nil {
			return err
		}
	}
	abs, err := filepath.Abs(*out)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(abs); err == nil {
		return fmt.Errorf("output already exists: %s", abs)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(abs), ".sppr-dataset-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	provenance, err := sourceVersion()
	if err != nil {
		return err
	}
	var m dataset.Manifest
	if external {
		m, err = dataset.WriteExternal(ctx, *catalogPath, *eventsPath, *sourceManifest, staging, provenance, options)
	} else {
		m, err = dataset.WriteWithOptions(ctx, cfg, staging, provenance, options)
	}
	if err != nil {
		return err
	}
	if _, err := os.Lstat(abs); err == nil {
		return fmt.Errorf("output appeared during generation: %s", abs)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(staging, abs); err != nil {
		return err
	}
	fmt.Printf("%s\n%s\n", abs, dataset.Describe(m))
	return nil
}

func sourceVersion() (map[string]string, error) {
	rootBytes, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return nil, fmt.Errorf("locate source checkout: %w", err)
	}
	root := strings.TrimSpace(string(rootBytes))
	var paths []string
	err = filepath.WalkDir(filepath.Join(root, "services/sppr"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && (strings.HasSuffix(path, ".go") || entry.Name() == "go.mod" || entry.Name() == "go.sum") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		rel, _ := filepath.Rel(root, path)
		fmt.Fprintf(h, "%s\x00%d\x00", rel, len(b))
		h.Write(b)
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return nil, err
	}
	return map[string]string{"git_head": strings.TrimSpace(string(head)), "sppr_source_sha256": hex.EncodeToString(h.Sum(nil)), "go_version": runtime.Version()}, nil
}
