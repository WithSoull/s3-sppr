package stand

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/loadgen"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
	metadata "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Outcome struct {
	loadgen.Result
	Scenario    string            `json:"scenario"`
	Seed        uint64            `json:"seed"`
	Policy      string            `json:"policy"`
	Budget      int64             `json:"budget_bytes"`
	Config      sim.Config        `json:"config"`
	StandConfig Config            `json:"stand_config"`
	Versions    map[string]string `json:"versions"`
	// Live service enforces placement invariants; a stand observation alone cannot
	// prove preservation or all transient capacity/budget states.
	Violations         *int `json:"violations"`
	InvariantsVerified bool `json:"invariants_verified"`
	// HistoryGaps is gaps_24h of the SPPR status after the run; a gap makes the
	// features around it unusable, so a run with gaps is not a fair comparison.
	HistoryGaps *int `json:"history_gaps"`
}
type runManifest struct {
	Project     string            `json:"compose_project_name"`
	Policy      string            `json:"policy"`
	GitSHA      string            `json:"git_sha"`
	Config      sim.Config        `json:"simulation"`
	StandConfig Config            `json:"stand"`
	Images      json.RawMessage   `json:"images"`
	Containers  json.RawMessage   `json:"containers"`
	Quota       map[string]any    `json:"sppr_quota"`
	Inputs      map[string]string `json:"input_sha256"`
}

func projectName(dir string) string {
	abs, _ := filepath.Abs(dir)
	h := sha256.Sum256([]byte(abs))
	return "sppr-exp-" + hex.EncodeToString(h[:10])
}
func InputHashes(c Config, sc sim.Config, scenario string, seed uint64) (map[string]string, error) {
	paths, err := c.Artifacts(sc, scenario, seed)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	paths["compose"] = c.ComposeFile
	paths["sppr-dockerfile"] = "services/sppr/Dockerfile"
	// Compose reads these tracked files even when ambient environment overrides
	// some settings. Record them as inputs rather than relying on git SHA alone.
	for _, service := range []string{"metadata", "storage", "users", "auth", "authz", "quota"} {
		paths["env-"+service] = "services/" + service + "/.env"
	}
	for key, path := range paths {
		path, err = filepath.EvalSymlinks(path)
		if err != nil {
			return nil, err
		}
		err := filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			h, e := dataset.HashFile(p)
			if e != nil {
				return e
			}
			rel, _ := filepath.Rel(path, p)
			out[key+"/"+rel] = h
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
func RunPair(ctx context.Context, c Config, sc sim.Config, dir, git string) error {
	artifacts, err := c.Artifacts(sc, sc.Generator.Scenario, sc.Generator.Seed)
	if err != nil {
		return err
	}
	inputs, err := InputHashes(c, sc, sc.Generator.Scenario, sc.Generator.Seed)
	if err != nil {
		return err
	}
	policies := sc.Policies
	budgets := sc.Budgets
	if c.Overhead {
		policies = OverheadPolicies
		budgets = []int64{sc.Budgets[0]}
	}
	results := filepath.Join(dir, "results")
	if err := os.Mkdir(results, 0755); err != nil {
		return err
	}
	type run struct {
		policy  string
		budget  int64
		dir     string
		compose compose
	}
	var runs []run
	// Rebuild all deterministic project descriptors and clean every project before
	// restarting an interrupted pair, including a project from a later policy.
	for _, policy := range policies {
		for _, budget := range budgets {
			sub := filepath.Join(dir, fmt.Sprintf("%s-%d", policy, budget))
			if err = os.Mkdir(sub, 0755); err != nil {
				return err
			}
			d, e := prepareCompose(ctx, c, sc, policy, budget, artifacts, sub, projectName(sub))
			if e != nil {
				return e
			}
			if _, e = d.run(ctx, "down", "-v", "--remove-orphans"); e != nil {
				return e
			}
			runs = append(runs, run{policy, budget, sub, d})
		}
	}
	for _, r := range runs {
		current, e := InputHashes(c, sc, sc.Generator.Scenario, sc.Generator.Seed)
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(inputs, current) {
			return fmt.Errorf("stand inputs changed during pair")
		}
		fmt.Printf("stand %s/%d %s budget=%d\n", sc.Generator.Scenario, sc.Generator.Seed, r.policy, r.budget)
		outcome, e := runOne(ctx, c, sc, r.compose, r.policy, r.budget, r.dir, git, inputs)
		if e != nil {
			return e
		}
		if err = writeJSON(filepath.Join(results, fmt.Sprintf("%s-%d.json", r.policy, r.budget)), outcome); err != nil {
			return err
		}
	}
	return nil
}
func runOne(ctx context.Context, c Config, sc sim.Config, d compose, policy string, budget int64, dir, git string, inputs map[string]string) (out Outcome, retErr error) {
	// Independent cleanup survives cancellation. Failure prevents a completed checkpoint.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		b, e := d.run(cleanup, "logs", "--no-color")
		retErr = errors.Join(retErr, e, os.WriteFile(filepath.Join(dir, "services.log"), b, 0600))
		_, e = d.run(cleanup, "down", "-v", "--remove-orphans")
		retErr = errors.Join(retErr, e)
	}()
	services := []string{"gateway", "metadata", "storage-hot", "storage-cold"}
	startup, cancel := context.WithTimeout(ctx, time.Duration(c.StartupSeconds*1e9))
	defer cancel()
	if err := kafkaReady(startup, d); err != nil {
		return out, err
	}
	if _, err := d.run(startup, append([]string{"up", "-d", "--build"}, services...)...); err != nil {
		return out, err
	}
	gw, err := d.endpoint(startup, "gateway", 8080)
	if err != nil {
		return out, err
	}
	md, err := d.endpoint(startup, "metadata", 50052)
	if err != nil {
		return out, err
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	gateway := api{"http://" + gw, client}
	if err = retry(startup, func() error { return gateway.request(startup, "GET", "/healthz", nil, nil) }); err != nil {
		return out, err
	}
	// Metadata capacities are persisted SQL seed values, not runtime env settings.
	sql := fmt.Sprintf("UPDATE storage_classes SET capacity_bytes=CASE name WHEN 'HOT' THEN %d ELSE %d END,max_utilization=%g WHERE name IN ('HOT','COLD');", sc.HotBytes, sc.ColdBytes, sc.Utilization)
	if _, err = d.run(startup, "exec", "-T", "postgres-metadata", "psql", "-U", "metadata_user", "-d", "metadata_db", "-v", "ON_ERROR_STOP=1", "-c", sql); err != nil {
		return out, err
	}
	conn, err := grpc.NewClient(md, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return out, err
	}
	defer conn.Close()
	meta := metadata.NewMetadataServiceClient(conn)
	bucket := "experiment-" + uuid.NewString()
	if err = retry(startup, func() error {
		_, e := meta.CreateBucket(startup, &metadata.CreateBucketRequest{Name: bucket, OwnerId: uuid.NewString()})
		return e
	}); err != nil {
		return out, err
	}
	lc := c.Load
	lc.Generator = sc.Generator
	seedClient := &http.Client{Timeout: time.Duration(lc.RequestTimeoutSeconds * 1e9), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer seedClient.CloseIdleConnections()
	if err = loadgen.Seed(ctx, seedClient, gateway.base, bucket, lc); err != nil {
		return out, err
	}
	// SPPR starts on the seeded catalogue: the bootstrap model is probed on catalogue
	// features once, on an empty journal. The Ingestor reads Kafka from the oldest offset.
	startup, cancelSPPR := context.WithTimeout(ctx, time.Duration(c.StartupSeconds*1e9))
	defer cancelSPPR()
	if policy != "no-extension" {
		if _, err = d.run(startup, "up", "-d", "--build", "sppr"); err != nil {
			return out, err
		}
	}
	var a api
	var container string
	if policy != "no-extension" {
		addr, e := d.endpoint(startup, "sppr", 8086)
		if e != nil {
			return out, e
		}
		a = api{"http://" + addr, client}
		if err = retry(startup, func() error { return a.request(startup, "GET", "/api/planner/status", nil, nil) }); err != nil {
			return out, err
		}
		if c.Overhead {
			if err = a.request(ctx, "POST", "/api/migration/pause", map[string]any{}, nil); err != nil {
				return out, err
			}
		}
		b, e := d.run(ctx, "ps", "-q", "sppr")
		if e != nil {
			return out, e
		}
		container = strings.TrimSpace(string(b))
		b, e = command(ctx, "inspect", "--format", `{{json .HostConfig}}`, container)
		if e != nil {
			return out, e
		}
		var host struct {
			NanoCPUs int64
			Memory   int64
		}
		if e = json.Unmarshal(b, &host); e != nil {
			return out, e
		}
		if host.NanoCPUs != 2e9 || host.Memory != 2<<30 {
			return out, fmt.Errorf("SPPR quota differs: %+v", host)
		}
		if e = os.WriteFile(filepath.Join(dir, "sppr-hostconfig.json"), b, 0600); e != nil {
			return out, e
		}
	}
	images, err := d.run(ctx, "images", "--format", "json")
	if err != nil {
		return out, err
	}
	containers, err := d.run(ctx, "ps", "--format", "json")
	if err != nil {
		return out, err
	}
	manifest := runManifest{Project: d.project, Policy: policy, GitSHA: git, Config: sc, StandConfig: c, Images: jsonArray(images), Containers: jsonArray(containers), Quota: map[string]any{"cpus": 2, "memory_bytes": int64(2 << 30), "present": policy != "no-extension"}, Inputs: inputs}
	if err = writeJSON(filepath.Join(dir, "manifest.json"), manifest); err != nil {
		return out, err
	}
	if policy != "no-extension" {
		if err = a.snapshot(ctx, filepath.Join(dir, "before")); err != nil {
			return out, err
		}
	}
	active, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	launch := func(f func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := f(active); e != nil && !errors.Is(e, context.Canceled) {
				errs <- e
				stop()
			}
		}()
	}
	if policy != "no-extension" {
		launch(func(ctx context.Context) error {
			return sampleStats(ctx, container, filepath.Join(dir, "stats.jsonl"), c.StatsSeconds)
		})
		if !c.Overhead {
			launch(func(ctx context.Context) error {
				return a.confirmations(ctx, time.Duration(sc.ConfirmSeconds)*time.Second)
			})
		}
		if policy == "sppr-update" {
			launch(func(ctx context.Context) error {
				if err := sleep(ctx, time.Duration(lc.WarmupSeconds/lc.TimeCompression*1e9)); err != nil {
					return err
				}
				return training(ctx, d, dir, c, sc)
			})
		}
	}
	raw := ""
	if c.RawLatencies {
		raw = filepath.Join(dir, "latencies.jsonl.gz")
	}
	r, runErr := loadgen.Run(active, seedClient, gateway.base, bucket, lc, raw)
	stop()
	wg.Wait()
	close(errs)
	for e := range errs {
		runErr = errors.Join(runErr, e)
	}
	out = Outcome{Result: r, Scenario: sc.Generator.Scenario, Seed: sc.Generator.Seed, Policy: policy, Budget: budget, Config: sc, StandConfig: c, Versions: map[string]string{"git_sha": git, "generator": "synthetic-v1"}}
	if policy != "no-extension" {
		runErr = errors.Join(runErr, a.snapshot(ctx, filepath.Join(dir, "after")))
		var st struct {
			History struct {
				Gaps *int `json:"gaps_24h"`
			} `json:"history"`
		}
		if b, e := os.ReadFile(filepath.Join(dir, "after", "status.json")); e == nil && json.Unmarshal(b, &st) == nil {
			out.HistoryGaps = st.History.Gaps
		}
	}
	// Even failed runs retain accounted load, before cleanup and before returning.
	runErr = errors.Join(runErr, writeJSON(filepath.Join(dir, "load-result.json"), out))
	return out, runErr
}
func sampleStats(ctx context.Context, container, path string, seconds float64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for {
		sampleCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		b, e := command(sampleCtx, "stats", "--no-stream", "--format", "{{json .}}", container)
		cancel()
		if e != nil {
			return e
		}
		var stats map[string]any
		if e = json.Unmarshal(b, &stats); e != nil {
			return e
		}
		if e = enc.Encode(map[string]any{"at": time.Now().UTC(), "stats": stats}); e != nil {
			return e
		}
		if err = sleep(ctx, time.Duration(seconds*1e9)); err != nil {
			return err
		}
	}
}
func training(ctx context.Context, d compose, dir string, c Config, sc sim.Config) error {
	f, err := os.Create(filepath.Join(dir, "training.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for n := 0; ; n++ {
		started := time.Now().UTC()
		dest := fmt.Sprintf("/tmp/model-update-%d", n)
		b, e := d.run(ctx, "exec", "-T", "sppr", "python", "-m", "sppr_ml", "train", "--data", "/artifacts/training", "--out", dest, "--params", string(sc.Params), "--fit-only")
		if err = os.WriteFile(filepath.Join(dir, fmt.Sprintf("training-%d.log", n)), b, 0600); err != nil {
			return err
		}
		if err = enc.Encode(map[string]any{"started": started, "finished": time.Now().UTC(), "success": e == nil, "error": fmt.Sprint(e), "export": dest}); err != nil {
			return err
		}
		if e != nil {
			return e
		}
		// Keep a successful export as evidence, then remove the container copy.
		local, err := filepath.Abs(filepath.Join(dir, fmt.Sprintf("model-update-%d", n)))
		if err != nil {
			return err
		}
		if _, err = d.run(ctx, "cp", "sppr:"+dest, local); err != nil {
			return err
		}
		if _, err = lgbm.Load(local, time.Hour); err != nil {
			return fmt.Errorf("updated export: %w", err)
		}
		if _, err = d.run(ctx, "exec", "-T", "sppr", "rm", "-rf", dest); err != nil {
			return err
		}
		if err = sleep(ctx, time.Until(started.Add(time.Duration(c.TrainingEverySeconds*1e9)))); err != nil {
			return err
		}
	}
}

// jsonArray accepts both output styles of `docker compose --format json`: one
// array (older Compose) or one object per line.
func jsonArray(b []byte) json.RawMessage {
	if json.Valid(b) {
		return b
	}
	var items []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(b))
	for {
		var v json.RawMessage
		if dec.Decode(&v) != nil {
			break
		}
		items = append(items, v)
	}
	out, _ := json.Marshal(items)
	return out
}

// accessTopics are created before Gateway starts: auto-creation elects a leader
// while the first events arrive, the producer buffer overflows and the history
// starts with gaps that make every feature window of the run unusable.
var accessTopics = []string{"object-read", "object-written"}

func kafkaReady(ctx context.Context, d compose) error {
	if _, err := d.run(ctx, "up", "-d", "--wait", "kafka"); err != nil {
		return err
	}
	for _, topic := range accessTopics {
		kafka := []string{"exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--topic", topic}
		if err := retry(ctx, func() error {
			_, e := d.run(ctx, append(kafka, "--create", "--if-not-exists", "--partitions", "1", "--replication-factor", "1")...)
			return e
		}); err != nil {
			return err
		}
		if err := retry(ctx, func() error {
			b, e := d.run(ctx, append(kafka, "--describe")...)
			if e != nil {
				return e
			}
			return leaderElected(string(b))
		}); err != nil {
			return err
		}
	}
	return nil
}

func leaderElected(describe string) error {
	partitions := 0
	for _, line := range strings.Split(describe, "\n") {
		i := strings.Index(line, "Leader:")
		if i < 0 {
			continue
		}
		partitions++
		f := strings.Fields(line[i+len("Leader:"):])
		if len(f) == 0 || f[0] == "-1" || f[0] == "none" {
			return fmt.Errorf("partition without leader: %s", strings.TrimSpace(line))
		}
	}
	if partitions == 0 {
		return fmt.Errorf("no partitions described")
	}
	return nil
}
