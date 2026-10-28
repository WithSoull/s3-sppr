// Command smoke-ml-update exercises retraining and failure retention on a dedicated stand.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/adminseed"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	gateway := flag.String("gateway", "http://localhost:8080", "Gateway URL")
	api := flag.String("sppr", "http://localhost:8086", "authenticated SPPR API")
	metadata := flag.String("metadata", adminseed.Env("METADATA_GRPC_ADDR", "localhost:50052"), "Metadata gRPC")
	warmup := flag.Duration("warmup", 40*time.Minute, "real read history; fixed 15m slices require at least 35m")
	timeout := flag.Duration("timeout", 60*time.Minute, "entire smoke deadline")
	fail := flag.String("failure-restart-command", "PLANNER_TRAIN_COMMAND=false docker compose --profile sppr up -d --no-deps --force-recreate sppr", "operator command to recreate SPPR with failing trainer")
	restore := flag.String("restore-restart-command", "docker compose --profile sppr up -d --no-deps --force-recreate sppr", "restore original trainer after failure test")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := run(ctx, strings.TrimRight(*gateway, "/"), strings.TrimRight(*api, "/"), *metadata, *warmup, *fail, *restore); err != nil {
		fmt.Fprintln(os.Stderr, "ML update smoke failed:", err)
		os.Exit(1)
	}
}

type smoke struct {
	http       *http.Client
	api, token string
	placements metadatav1.PlacementServiceClient
}
type plan struct {
	ModelVersion string `json:"model_version"`
	ID           string `json:"plan_id"`
	Revision     int64  `json:"revision"`
	Status       string `json:"status"`
	Reason       string `json:"reason"`
}
type rec struct {
	ID          string            `json:"recommendation_id"`
	Version     string            `json:"version_id"`
	Target      string            `json:"target_class"`
	Selected    bool              `json:"selected"`
	Deps        []string          `json:"depends_on"`
	Explanation model.Explanation `json:"explanation"`
}
type command struct {
	ID        string   `json:"command_id"`
	Plan      string   `json:"plan_id"`
	Status    string   `json:"status"`
	Code      string   `json:"code"`
	Actions   []string `json:"actions"`
	Missing   []string `json:"missing"`
	Initiator string   `json:"initiator"`
}

func (s *smoke) request(ctx context.Context, method, path, token string, body any, out any, want int) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, s.api+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return err
	}
	if res.StatusCode != want {
		return fmt.Errorf("%s %s: HTTP %d, want %d: %s", method, path, res.StatusCode, want, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("%s decode: %w", path, err)
		}
	}
	return nil
}
func (s *smoke) json(ctx context.Context, method, path string, body, out any, want int) error {
	return s.request(ctx, method, path, s.token, body, out, want)
}
func (s *smoke) execute(ctx context.Context, path string, body map[string]any, want int) (command, error) {
	body["command_id"] = uuid.NewString()
	var cmd command
	err := s.json(ctx, http.MethodPost, "/api/planner/"+path, body, &cmd, want)
	return cmd, err
}
func (s *smoke) recompute(ctx context.Context) (plan, []rec, error) {
	cmd, err := s.execute(ctx, "plans/recompute", map[string]any{}, 202)
	if err != nil {
		return plan{}, nil, err
	}
	err = wait(ctx, func() (bool, error) {
		err := s.json(ctx, "GET", "/api/planner/commands/"+cmd.ID, nil, &cmd, 200)
		return cmd.Plan != "", err
	})
	if err != nil {
		return plan{}, nil, err
	}
	var p plan
	if err := s.json(ctx, "GET", "/api/planner/plans/"+cmd.Plan, nil, &p, 200); err != nil {
		return p, nil, err
	}
	if p.Status != "ready" {
		return p, nil, fmt.Errorf("plan %s: %s/%s; inspect history/stand", p.ID, p.Status, p.Reason)
	}
	var all []rec
	token := ""
	for {
		var page struct {
			Recs []rec  `json:"recommendations"`
			Next string `json:"next_page_token"`
		}
		if err := s.json(ctx, "GET", "/api/planner/plans/"+p.ID+"/recommendations?selected=true&page_size=100&page_token="+token, nil, &page, 200); err != nil {
			return p, nil, err
		}
		all = append(all, page.Recs...)
		if page.Next == "" {
			break
		}
		token = page.Next
	}
	return p, all, nil
}
func (s *smoke) readyPlan(ctx context.Context, predicate func([]rec) bool) (plan, []rec, error) {
	var p plan
	var r []rec
	err := wait(ctx, func() (bool, error) {
		var err error
		p, r, err = s.recompute(ctx)
		return err == nil && predicate(r), err
	})
	return p, r, err
}
func wait(ctx context.Context, f func() (bool, error)) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ok, err := f()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (s *smoke) object(ctx context.Context, method, target string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	res, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: %d %s", method, target, res.StatusCode, raw)
	}
	return raw, nil
}
func clone(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
func (s *smoke) settings(ctx context.Context, settings map[string]any) error {
	body := clone(settings)
	body["command_id"] = uuid.NewString()
	return s.json(ctx, "PUT", "/api/planner/settings", body, nil, 202)
}
func (s *smoke) finish(ctx context.Context, cmd command) error {
	for _, id := range cmd.Actions {
		if err := wait(ctx, func() (bool, error) {
			var a struct {
				Status string            `json:"status"`
				Reason string            `json:"reason"`
				Events []json.RawMessage `json:"events"`
			}
			if err := s.json(ctx, "GET", "/api/migration/actions/"+id, nil, &a, 200); err != nil {
				return false, err
			}
			if a.Status == "failed" || a.Status == "cancelled" {
				return false, fmt.Errorf("action %s %s/%s", id, a.Status, a.Reason)
			}
			return a.Status == "succeeded" && len(a.Events) > 0, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func restart(ctx context.Context, command string) error {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
func (s *smoke) lifecycle(ctx context.Context) (model.ModelLifecycleStatus, error) {
	var st struct {
		Lifecycle model.ModelLifecycleStatus `json:"model_lifecycle"`
	}
	err := s.json(ctx, "GET", "/api/planner/status", nil, &st, 200)
	return st.Lifecycle, err
}
func (s *smoke) retrain(ctx context.Context) (model.ModelLifecycleStatus, error) {
	before, err := s.lifecycle(ctx)
	if err != nil {
		return before, err
	}
	id := uuid.NewString()
	body := map[string]any{"command_id": id}
	var first, again command
	if err := s.json(ctx, "POST", "/api/planner/model/retrain", body, &first, 202); err != nil {
		return before, err
	}
	if err := s.json(ctx, "POST", "/api/planner/model/retrain", body, &again, 202); err != nil {
		return before, err
	}
	if first.ID != again.ID || first.Status != again.Status {
		return before, fmt.Errorf("retrain command replay differs")
	}
	var after model.ModelLifecycleStatus
	err = wait(ctx, func() (bool, error) {
		var e error
		after, e = s.lifecycle(ctx)
		if e != nil {
			return false, e
		}
		newer := after.LastAttempt != nil && (before.LastAttempt == nil || after.LastAttempt.ModelVersion != before.LastAttempt.ModelVersion)
		return newer && !after.Training, e
	})
	return after, err
}
func run(ctx context.Context, gateway, api, metadata string, warmup time.Duration, fail, restore string) (result error) {
	if warmup < 35*time.Minute {
		return fmt.Errorf("warmup must be >=35m for fixed 15m train and validation slices")
	}
	if fail == "" || restore == "" {
		return fmt.Errorf("failure and restore restart commands are required")
	}
	conn, err := grpc.NewClient(metadata, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	admin, err := adminseed.Seed(ctx, adminseed.FromEnv(), true)
	if err != nil {
		return err
	}
	s := &smoke{http: &http.Client{Timeout: 10 * time.Second}, api: api, token: admin.Token, placements: metadatav1.NewPlacementServiceClient(conn)}
	defer s.http.CloseIdleConnections()
	if err := wait(ctx, func() (bool, error) {
		e := s.json(ctx, "GET", "/api/planner/status", nil, nil, 200)
		return e == nil, nil
	}); err != nil {
		return err
	}
	var st struct {
		Hot struct {
			Used int64 `json:"used_bytes"`
		} `json:"hot"`
		Pending int `json:"pending_actions"`
		Running int `json:"running_actions"`
	}
	if err := s.json(ctx, "GET", "/api/planner/status", nil, &st, 200); err != nil {
		return err
	}
	if st.Hot.Used != 0 || st.Pending != 0 || st.Running != 0 {
		return fmt.Errorf("dedicated empty-HOT stand without active actions required")
	}
	var old map[string]any
	if err := s.json(ctx, "GET", "/api/planner/settings", nil, &old, 200); err != nil {
		return err
	}
	if old["horizon_seconds"] != float64(120) {
		return fmt.Errorf("stand must start with PLANNER_HORIZON=2m and stored horizon=120")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		result = errors.Join(result, s.settings(cleanup, old))
	}()
	settings := clone(old)
	settings["min_gain_ms"] = 0
	settings["penalty_ms_per_mib"] = 0
	if err := s.settings(ctx, settings); err != nil {
		return err
	}
	bucket := "ml-update-" + uuid.NewString()
	meta := metadatav1.NewMetadataServiceClient(conn)
	if _, err := meta.CreateBucket(ctx, &metadatav1.CreateBucketRequest{Name: bucket, OwnerId: admin.ID}); err != nil {
		return err
	}
	const count = 16
	urls := make([]string, count)
	bodies := make([][]byte, count)
	versions := make([]string, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("object-%d.bin", i)
		urls[i] = gateway + "/objects/" + bucket + "/" + key
		bodies[i] = bytes.Repeat([]byte{byte(i), 0, 255, 42}, 16384)
		if _, err := s.object(ctx, "PUT", urls[i], bodies[i]); err != nil {
			return err
		}
		obj, err := meta.GetObjectMeta(ctx, &metadatav1.GetObjectMetaRequest{BucketName: bucket, Key: key})
		if err != nil {
			return err
		}
		versions[i] = obj.GetVersionId()
		if obj.GetStorageClass() != "COLD" {
			return fmt.Errorf("initial placement is not COLD")
		}
	}
	fmt.Printf("Collecting %s of real reads in %s (fixed 15m slices)\n", warmup, bucket)
	until := time.NewTimer(warmup)
	defer until.Stop()
	reads := time.NewTicker(2 * time.Second)
	defer reads.Stop()
readLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-until.C:
			break readLoop
		case <-reads.C:
			for i := 0; i < 4; i++ {
				body, err := s.object(ctx, "GET", urls[i], nil)
				if err != nil {
					return err
				}
				if !bytes.Equal(body, bodies[i]) {
					return fmt.Errorf("read bytes differ")
				}
			}
		}
	}
	admin, err = adminseed.Seed(ctx, adminseed.FromEnv(), true)
	if err != nil {
		return err
	}
	s.token = admin.Token
	after, err := s.retrain(ctx)
	if err != nil {
		return err
	}
	if after.Active == nil || after.LastAttempt == nil || after.LastAttempt.Status != "active" {
		return fmt.Errorf("training did not activate a model: %+v; stage 5 Coverage must advance", after)
	}
	version := after.Active.ModelVersion
	p, recs, err := s.recompute(ctx)
	if err != nil {
		return err
	}
	if p.ModelVersion != version {
		return fmt.Errorf("plan has model %s, want %s", p.ModelVersion, version)
	}
	found := false
	for _, r := range recs {
		if r.Version == versions[0] && r.Target == "HOT" {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("ML plan did not select the read object for HOT")
	}
	cmd, err := s.execute(ctx, "plans/"+p.ID+"/confirm", map[string]any{}, 202)
	if err != nil {
		return err
	}
	if len(cmd.Actions) == 0 {
		return fmt.Errorf("confirmation returned no actions")
	}
	if err := s.finish(ctx, cmd); err != nil {
		return err
	}
	placement, err := s.placements.GetPlacement(ctx, &metadatav1.GetPlacementRequest{VersionId: versions[0]})
	if err != nil {
		return err
	}
	if placement.GetPlacement().GetStorageClass() != "HOT" {
		return fmt.Errorf("confirmed object is not HOT")
	}
	body, err := s.object(ctx, "GET", urls[0], nil)
	if err != nil {
		return err
	}
	if !bytes.Equal(body, bodies[0]) {
		return fmt.Errorf("migrated bytes differ")
	}
	// This is an operator-supplied process restart, never a public API for arbitrary commands.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := restart(cleanup, restore); err != nil {
			result = errors.Join(result, err)
			return
		}
		// The deferred settings restore runs next and needs the recreated SPPR.
		result = errors.Join(result, wait(cleanup, func() (bool, error) {
			return s.json(cleanup, "GET", "/api/planner/status", nil, nil, 200) == nil, nil
		}))
	}()
	if err := restart(ctx, fail); err != nil {
		return err
	}
	if err := wait(ctx, func() (bool, error) {
		e := s.json(ctx, "GET", "/api/planner/status", nil, nil, 200)
		return e == nil, nil
	}); err != nil {
		return err
	}
	after, err = s.retrain(ctx)
	if err != nil {
		return err
	}
	if after.LastAttempt == nil || after.LastAttempt.Status != "rejected" || after.LastAttempt.Reason == "" || after.Active == nil || after.Active.ModelVersion != version {
		return fmt.Errorf("failed training did not retain %s: %+v", version, after)
	}
	var journal struct {
		Versions []model.ModelVersion `json:"versions"`
	}
	if err := s.json(ctx, "GET", "/api/planner/model/versions", nil, &journal, 200); err != nil {
		return err
	}
	found = false
	for _, v := range journal.Versions {
		if v.ModelVersion == after.LastAttempt.ModelVersion && v.Status == "rejected" {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("failed attempt absent from journal")
	}
	p, _, err = s.recompute(ctx)
	if err != nil {
		return err
	}
	if p.ModelVersion != version {
		return fmt.Errorf("plan after failure changed model")
	}
	fmt.Println("PASS: retrain, command replay, versioned plan, confirmation, migration/read, restart and failed training retention; bucket:", bucket)
	return nil
}
