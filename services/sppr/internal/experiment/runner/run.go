// Package runner owns an experiment series and its atomic per-pair checkpoints.
package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/stand"
)

type Config struct {
	Mode      string   `json:"mode"`
	Scenarios []string `json:"scenarios"`
	Seeds     []uint64 `json:"seeds"`
	Workers   int      `json:"workers"`
	// DropCache removes the large replay caches of a finished pair before it is
	// hashed; models, baseline maps, logs and the first (train-only) dataset stay
	// for the stand and overhead series.
	DropCache  bool         `json:"drop_cache"`
	Simulation sim.Config   `json:"simulation"`
	Stand      stand.Config `json:"stand"`
}
type Manifest struct {
	Version   string                       `json:"version"`
	Config    Config                       `json:"config"`
	ConfigSHA string                       `json:"config_sha256"`
	GitSHA    string                       `json:"git_sha"`
	Dirty     bool                         `json:"dirty"`
	Command   []string                     `json:"command"`
	Started   time.Time                    `json:"started"`
	Finished  *time.Time                   `json:"finished,omitempty"`
	BinarySHA string                       `json:"binary_sha256"`
	SHA256    map[string]string            `json:"sha256,omitempty"`
	Inputs    map[string]map[string]string `json:"input_sha256,omitempty"`
}

func Load(path string) (Config, error) {
	c := Config{Mode: "sim", Workers: 1, Simulation: sim.DefaultConfig(), Stand: stand.DefaultConfig()}
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, err
	}
	if dec.Decode(new(any)) != io.EOF {
		return c, fmt.Errorf("config must contain one JSON object")
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.Mode != "sim" && c.Mode != "stand" {
		return fmt.Errorf("unknown mode %q", c.Mode)
	}
	if c.Mode == "stand" {
		if c.Stand.Overhead && len(c.Scenarios) != 1 {
			return fmt.Errorf("overhead requires exactly one scenario")
		}
		if c.Workers != 1 {
			return fmt.Errorf("stand runs must be serial (workers=1)")
		}
		if err := c.Stand.Validate(c.Simulation); err != nil {
			return err
		}
	}
	if c.Workers < 1 || c.Workers > 2 || len(c.Scenarios) == 0 || len(c.Seeds) == 0 {
		return fmt.Errorf("invalid matrix/workers (1 or 2)")
	}
	seen := map[string]bool{}
	for _, s := range c.Scenarios {
		if seen[s] {
			return fmt.Errorf("duplicate scenario")
		}
		seen[s] = true
		sc := c.Simulation
		sc.Generator.Scenario = s
		if c.Mode == "stand" {
			sc.Generator = c.Stand.Load.Generator
			sc.Generator.Scenario = s
			lc := c.Stand.Load
			lc.Generator = sc.Generator
			if err := lc.Validate(); err != nil {
				return err
			}
		}
		if err := sc.Validate(); err != nil {
			return err
		}
	}
	seeds := map[uint64]bool{}
	for _, s := range c.Seeds {
		if seeds[s] {
			return fmt.Errorf("duplicate seed")
		}
		seeds[s] = true
	}
	return nil
}
func Revision() (string, bool) {
	if s := os.Getenv("SPPR_GIT_SHA"); s != "" {
		return s, false
	}
	b, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown", true
	}
	st, err := exec.Command("git", "status", "--porcelain").Output()
	return strings.TrimSpace(string(b)), err != nil || len(st) > 0
}
func jsonFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func hashes(dir string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if name == "pair.json" {
			return nil
		}
		h, err := dataset.HashFile(path)
		if err != nil {
			return err
		}
		out[name] = h
		return nil
	})
	return out, err
}
func verify(dir string, want map[string]string) error {
	got, err := hashes(dir)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("checkpoint hashes differ: %s", dir)
	}
	return nil
}
func Run(ctx context.Context, c Config, out string, resume bool) error {
	if err := c.Validate(); err != nil {
		return err
	}
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	sha := hex.EncodeToString(sum[:])
	git, dirty := Revision()
	exe, _ := os.Executable()
	binary, _ := dataset.HashFile(exe)
	version := sim.Version
	var inputs map[string]map[string]string
	if c.Mode == "stand" {
		inputs = map[string]map[string]string{}
		version = stand.Version
		for _, scenario := range c.Scenarios {
			for _, seed := range c.Seeds {
				h, err := stand.InputHashes(c.Stand, c.Simulation, scenario, seed)
				if err != nil {
					return err
				}
				inputs[fmt.Sprintf("%s-%d", scenario, seed)] = h
			}
		}
	}
	m := Manifest{Inputs: inputs, Version: version, Config: c, ConfigSHA: sha, GitSHA: git, Dirty: dirty, Command: os.Args, Started: time.Now().UTC(), BinarySHA: binary}
	if resume {
		raw, err := os.ReadFile(filepath.Join(out, "manifest.json"))
		if err != nil {
			return err
		}
		var old Manifest
		if err := json.Unmarshal(raw, &old); err != nil {
			return err
		}
		if old.ConfigSHA != sha || old.GitSHA != git || old.Version != version || old.BinarySHA != binary || !reflect.DeepEqual(old.Inputs, m.Inputs) {
			return fmt.Errorf("resume requires same configuration, revision and executable")
		}
		m = old
	} else {
		if err := os.Mkdir(out, 0755); err != nil {
			return fmt.Errorf("new series required (use -resume for an interrupted series): %w", err)
		}
		if err := os.Mkdir(filepath.Join(out, "pairs"), 0755); err != nil {
			return err
		}
		if err := jsonFile(filepath.Join(out, "manifest.json"), m); err != nil {
			return err
		}
	}
	// The kernel releases this advisory lock even after SIGKILL. The file is
	// retained so another process cannot lock a different inode at the same path.
	lock := filepath.Join(out, ".lock")
	f, err := os.OpenFile(lock, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("series is running: %w", err)
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type pair struct {
		s    string
		seed uint64
	}
	jobs := make(chan pair)
	errs := make(chan error, c.Workers)
	var wg sync.WaitGroup
	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if ctx.Err() != nil {
					return
				}
				name := fmt.Sprintf("%s-%d", p.s, p.seed)
				final := filepath.Join(out, "pairs", name)
				if raw, err := os.ReadFile(filepath.Join(final, "pair.json")); err == nil {
					var checkpoint map[string]string
					if err = json.Unmarshal(raw, &checkpoint); err == nil {
						err = verify(final, checkpoint)
					}
					if err != nil {
						errs <- err
						cancel()
						return
					}
					fmt.Printf("%s: verified checkpoint\n", name)
					continue
				} else if !os.IsNotExist(err) {
					errs <- err
					cancel()
					return
				}
				tmp := final + ".partial"
				if resume {
					if err := os.RemoveAll(tmp); err != nil {
						errs <- err
						cancel()
						return
					}
				}
				err := os.Mkdir(tmp, 0755)
				if err == nil {
					sc := c.Simulation
					if c.Mode == "stand" {
						sc.Generator = c.Stand.Load.Generator
					}
					sc.Generator.Scenario, sc.Generator.Seed = p.s, p.seed
					t := time.Now()
					if c.Mode == "stand" {
						err = stand.RunPair(ctx, c.Stand, sc, tmp, git)
					} else {
						_, err = sim.Prepare(ctx, sc, filepath.Join(tmp, "cache"))
						if err == nil {
							var results []sim.Result
							results, err = sim.Replay(ctx, sc, filepath.Join(tmp, "cache"))
							if err == nil {
								err = sim.WriteResults(filepath.Join(tmp, "results"), results, git)
							}
							if err == nil && c.DropCache {
								err = dropCache(filepath.Join(tmp, "cache"))
							}
							if err == nil {
								for _, r := range results {
									fmt.Printf("%s %s budget=%d MiB reads=%d mean=%.4f ms p95=%.4f ms HOT=%.4f moved=%d violations=%d\n", name, r.Policy, r.Budget/(1<<20), r.Latency.Reads, r.Latency.MeanMs, r.Latency.P95Ms, r.HotShare, r.Migrations, r.Violations)
								}
							}
						}
					}
					if err == nil {
						var h map[string]string
						h, err = hashes(tmp)
						if err == nil {
							err = jsonFile(filepath.Join(tmp, "pair.json"), h)
						}
						if err == nil {
							err = os.Rename(tmp, final)
						}
					}
					fmt.Printf("%s: %.3f s\n", name, time.Since(t).Seconds())
				}
				if err != nil {
					errs <- fmt.Errorf("%s: %w", name, err)
					cancel()
					return
				}
			}
		}()
	}
sending:
	for _, s := range c.Scenarios {
		for _, seed := range c.Seeds {
			select {
			case jobs <- pair{s, seed}:
			case <-ctx.Done():
				break sending
			}
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	done := time.Now().UTC()
	m.Finished = &done
	m.SHA256 = map[string]string{}
	for _, s := range c.Scenarios {
		for _, seed := range c.Seeds {
			name := fmt.Sprintf("%s-%d", s, seed)
			h, err := dataset.HashFile(filepath.Join(out, "pairs", name, "pair.json"))
			if err != nil {
				return err
			}
			m.SHA256[name] = h
		}
	}
	return jsonFile(filepath.Join(out, "manifest.json"), m)
}

func dropCache(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	first := ""
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "train-") && (first == "" || e.Name() < first) {
			first = e.Name()
		}
	}
	for _, e := range entries {
		n := e.Name()
		if strings.HasSuffix(n, ".gob") || (e.IsDir() && strings.HasPrefix(n, "train-") && n != first) {
			if err := os.RemoveAll(filepath.Join(dir, n)); err != nil {
				return err
			}
		}
	}
	return nil
}
