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

func run() error {
	demo := flag.Bool("demo", false, "print hand-checkable lesson rows to stdout")
	configPath := flag.String("config", "", "generator JSON configuration")
	out := flag.String("out", "", "new output directory; existing paths are refused")
	scenario := flag.String("scenario", "", "override scenario")
	seed := flag.Int64("seed", -1, "override seed (0..9 for acceptance runs)")
	flag.Parse()
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
	if *configPath == "" || *out == "" {
		return fmt.Errorf("use -demo or -config <json> -out <new-directory>")
	}
	f, err := os.Open(*configPath)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	var cfg synthetic.Config
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
	m, err := dataset.Write(ctx, cfg, staging, provenance)
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
