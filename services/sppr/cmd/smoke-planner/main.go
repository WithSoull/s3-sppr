// Command smoke-planner checks the authenticated administrator loop on a dedicated stand.
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
	sppr := flag.String("sppr", "http://localhost:8086", "SPPR URL; authenticated Planner and executor required")
	metadata := flag.String("metadata", adminseed.Env("METADATA_GRPC_ADDR", "localhost:50052"), "Metadata gRPC")
	count := flag.Int("objects", 3, "number of equal-sized objects (at least 2)")
	timeout := flag.Duration("timeout", 15*time.Minute, "whole smoke deadline including cleanup")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := run(ctx, strings.TrimRight(*gateway, "/"), strings.TrimRight(*sppr, "/"), *metadata, *count); err != nil {
		fmt.Fprintln(os.Stderr, "planner smoke failed:", err)
		os.Exit(1)
	}
}

type smoke struct {
	http       *http.Client
	api, token string
	placements metadatav1.PlacementServiceClient
}
type plan struct {
	ID       string `json:"plan_id"`
	Revision int64  `json:"revision"`
	Status   string `json:"status"`
	Reason   string `json:"reason"`
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
func run(ctx context.Context, gateway, api, metadata string, count int) (result error) {
	if count < 2 || count > 100 {
		return fmt.Errorf("-objects must be 2..100")
	}
	conn, err := grpc.NewClient(metadata, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	s := &smoke{http: &http.Client{Timeout: 10 * time.Second}, api: api, placements: metadatav1.NewPlacementServiceClient(conn)}
	defer s.http.CloseIdleConnections()
	if err := wait(ctx, func() (bool, error) {
		err := s.request(ctx, "GET", "/readyz", "", nil, nil, 200)
		return err == nil, nil
	}); err != nil {
		return err
	}
	cfg := adminseed.FromEnv()
	admin, err := adminseed.Seed(ctx, cfg, true)
	if err != nil {
		return err
	}
	s.token = admin.Token
	fmt.Printf("Seed admin: %s / %s, user %s\n", cfg.Email, cfg.Password, admin.ID)
	if err := wait(ctx, func() (bool, error) {
		err := s.json(ctx, "GET", "/api/planner/status", nil, nil, 200)
		return err == nil, nil
	}); err != nil {
		return err
	}
	for _, path := range []string{"/api/planner/status", "/api/migration/settings"} {
		if err := s.request(ctx, "GET", path, "", nil, nil, 401); err != nil {
			return err
		}
	}
	noncfg := cfg
	noncfg.Email = "planner-nonadmin-" + uuid.NewString() + "@example.test"
	non, err := adminseed.Seed(ctx, noncfg, false)
	if err != nil {
		return err
	}
	// This must be checked on a write endpoint: C-04.
	if err := s.request(ctx, "POST", "/api/planner/plans/"+uuid.NewString()+"/confirm", non.Token, map[string]any{"command_id": uuid.NewString()}, nil, 403); err != nil {
		return err
	}
	fmt.Println("C-04: 401 without a token, 403 for a non-admin confirmation")
	var st struct {
		Hot struct {
			Capacity int64 `json:"capacity_bytes"`
			Used     int64 `json:"used_bytes"`
		} `json:"hot"`
		Pending int `json:"pending_actions"`
		Running int `json:"running_actions"`
	}
	if err := s.json(ctx, "GET", "/api/planner/status", nil, &st, 200); err != nil {
		return err
	}
	if st.Hot.Used != 0 || st.Pending != 0 || st.Running != 0 {
		return fmt.Errorf("dedicated stand required: HOT must be empty and no active actions; used=%d pending=%d running=%d", st.Hot.Used, st.Pending, st.Running)
	}
	var old map[string]any
	if err := s.json(ctx, "GET", "/api/planner/settings", nil, &old, 200); err != nil {
		return err
	}
	var oldMigration struct {
		Interval int64 `json:"min_interval_seconds"`
		Paused   bool  `json:"paused"`
	}
	if err := s.json(ctx, "GET", "/api/migration/settings", nil, &oldMigration, 200); err != nil {
		return err
	}
	var oldBudget struct {
		Bytes  int64 `json:"bytes"`
		Window int64 `json:"window_seconds"`
		Spent  int64 `json:"spent"`
	}
	if err := s.json(ctx, "GET", "/api/migration/budget", nil, &oldBudget, 200); err != nil {
		return err
	}
	// Restore persisted values even when a later assertion fails; surface failures.
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result = errors.Join(result, s.settings(restore, old), s.json(restore, "PUT", "/api/migration/settings", map[string]any{"min_interval_seconds": oldMigration.Interval}, nil, 200), s.json(restore, "PUT", "/api/migration/budget", map[string]any{"bytes": oldBudget.Bytes, "window_seconds": oldBudget.Window}, nil, 200))
		path := "resume"
		if oldMigration.Paused {
			path = "pause"
		}
		result = errors.Join(result, s.json(restore, "POST", "/api/migration/"+path, map[string]any{}, nil, 200))
	}()
	const objectSize = 65536
	if st.Hot.Capacity < 2*objectSize {
		return fmt.Errorf("HOT capacity too small")
	}
	current := clone(old)
	current["hot_utilization"] = float64(objectSize) * 1.5 / float64(st.Hot.Capacity)
	current["horizon_seconds"] = 3600
	current["hot_fixed_ms"] = 1
	current["hot_mib_per_ms"] = 0.5
	current["cold_fixed_ms"] = 20
	current["cold_mib_per_ms"] = 0.05
	current["cold_utilization"] = 0.9
	current["min_gain_ms"] = 0
	current["penalty_ms_per_mib"] = 0
	if err := s.settings(ctx, current); err != nil {
		return err
	}
	if err := s.json(ctx, "PUT", "/api/migration/settings", map[string]any{"min_interval_seconds": 0}, nil, 200); err != nil {
		return err
	}
	if err := s.json(ctx, "PUT", "/api/migration/budget", map[string]any{"bytes": oldBudget.Spent + 64*model.MiB, "window_seconds": oldBudget.Window}, nil, 200); err != nil {
		return err
	}
	if err := s.json(ctx, "POST", "/api/migration/resume", map[string]any{}, nil, 200); err != nil {
		return err
	}
	bucket := "planner-smoke-" + uuid.NewString()
	meta := metadatav1.NewMetadataServiceClient(conn)
	if _, err := meta.CreateBucket(ctx, &metadatav1.CreateBucketRequest{Name: bucket, OwnerId: admin.ID}); err != nil {
		return err
	}
	fmt.Println("Smoke bucket:", bucket)
	urls := make([]string, count)
	bodies := make([][]byte, count)
	versions := make([]string, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("object-%d.bin", i)
		urls[i] = gateway + "/objects/" + bucket + "/" + key
		bodies[i] = bytes.Repeat([]byte{byte(i), 0, 255, 42}, objectSize/4)
		if _, err := s.object(ctx, "PUT", urls[i], bodies[i]); err != nil {
			return err
		}
		m, err := meta.GetObjectMeta(ctx, &metadatav1.GetObjectMetaRequest{BucketName: bucket, Key: key})
		if err != nil {
			return err
		}
		if m.GetStorageClass() != "COLD" {
			return fmt.Errorf("initial class is %s", m.GetStorageClass())
		}
		versions[i] = m.GetVersionId()
	}
	reads := func(index, n int) error {
		for i := 0; i < n; i++ {
			raw, err := s.object(ctx, "GET", urls[index], nil)
			if err != nil {
				return err
			}
			if !bytes.Equal(raw, bodies[index]) {
				return fmt.Errorf("bytes differ for object %d", index)
			}
		}
		return nil
	}
	if err := reads(0, 12); err != nil {
		return err
	}
	has := func(rs []rec, version, target string) bool {
		for _, r := range rs {
			if r.Selected && r.Version == version && r.Target == target {
				return true
			}
		}
		return false
	}
	p, _, err := s.readyPlan(ctx, func(rs []rec) bool { return has(rs, versions[0], "HOT") })
	if err != nil {
		return err
	}
	stale := p
	p, _, err = s.readyPlan(ctx, func(rs []rec) bool { return has(rs, versions[0], "HOT") })
	if err != nil {
		return err
	}
	refused, err := s.execute(ctx, "plans/"+stale.ID+"/confirm", map[string]any{}, 409)
	if err != nil {
		return err
	}
	if refused.Code != "PLAN_STALE" {
		return fmt.Errorf("C-03: code %s", refused.Code)
	}
	fmt.Println("C-03: PLAN_STALE for old revision")
	accepted, err := s.execute(ctx, "plans/"+p.ID+"/confirm", map[string]any{}, 202)
	if err != nil {
		return err
	}
	if err := s.finish(ctx, accepted); err != nil {
		return err
	}
	if err := s.verifyHot(ctx, versions[0]); err != nil {
		return err
	}
	if err := reads(0, 1); err != nil {
		return err
	}
	fmt.Println("Confirmed promotion succeeded; Gateway returned the same bytes from HOT")
	// A larger new demand makes object 0 relatively cold; EWMA need not sleep an hour.
	if err := reads(1, 120); err != nil {
		return err
	}
	p, rs, err := s.readyPlan(ctx, func(rs []rec) bool {
		for _, r := range rs {
			if r.Version == versions[1] && r.Target == "HOT" && len(r.Deps) > 0 {
				return true
			}
		}
		return false
	})
	if err != nil {
		return err
	}
	var promotion rec
	for _, r := range rs {
		if r.Version == versions[1] && r.Target == "HOT" {
			promotion = r
		}
	}
	refused, err = s.execute(ctx, "plans/"+p.ID+"/confirm", map[string]any{"recommendation_ids": []string{promotion.ID}}, 409)
	if err != nil {
		return err
	}
	if refused.Code != "DEPENDENCY_REQUIRED" || len(refused.Missing) == 0 {
		return fmt.Errorf("C-02: missing dependency result: %+v", refused)
	}
	fmt.Println("C-02: DEPENDENCY_REQUIRED with missing actions", refused.Missing)
	accepted, err = s.execute(ctx, "plans/"+p.ID+"/confirm", map[string]any{}, 202)
	if err != nil {
		return err
	}
	if err := s.finish(ctx, accepted); err != nil {
		return err
	}
	// Promotion may complete only after every eviction's HOT source is deleted.
	for _, dep := range promotion.Deps {
		copies, err := s.placements.ListMigrationCopies(ctx, &metadatav1.ListMigrationCopiesRequest{MigrationId: dep})
		if err != nil {
			return err
		}
		cleaned := false
		for _, copy := range copies.GetCopies() {
			if copy.GetRole() == "source" && copy.GetStorageClass() == "HOT" && copy.GetState() == "deleted" {
				cleaned = true
			}
		}
		if !cleaned {
			return fmt.Errorf("dependency %s succeeded without releasing its HOT source", dep)
		}
	}
	if err := s.verifyHot(ctx, versions[1]); err != nil {
		return err
	}
	if err := reads(1, 1); err != nil {
		return err
	}
	cooled, err := s.placements.GetPlacement(ctx, &metadatav1.GetPlacementRequest{VersionId: versions[0]})
	if err != nil {
		return err
	}
	if cooled.GetPlacement().GetStorageClass() != "COLD" {
		return fmt.Errorf("cooled version did not return to COLD")
	}
	if err := reads(0, 1); err != nil {
		return err
	}
	// C-06 verifies persisted edges rather than only printing UUIDs.
	var journal command
	if err := s.json(ctx, "GET", "/api/planner/commands/"+accepted.ID, nil, &journal, 200); err != nil {
		return err
	}
	found := false
	for _, id := range journal.Actions {
		if id == promotion.ID {
			found = true
		}
	}
	if !found || journal.Plan != p.ID || journal.Initiator != admin.ID {
		return fmt.Errorf("C-06: command does not link plan, user and recommendation")
	}
	for _, path := range []string{"/admin/recommendations/" + promotion.ID + "?plan_id=" + p.ID, "/admin/commands/" + journal.ID, "/admin/migrations/" + promotion.ID} {
		req, _ := http.NewRequestWithContext(ctx, "GET", api+path, nil)
		req.Header.Set("Authorization", "Bearer "+s.token)
		resp, err := s.http.Do(req)
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		resp.Body.Close()
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 || !bytes.Contains(raw, []byte(promotion.ID)) {
			return fmt.Errorf("C-06: panel path %s does not show action", path)
		}
	}
	fmt.Printf("C-06: %s/admin/recommendations/%s?plan_id=%s -> command %s -> action journal %s\n", api, promotion.ID, p.ID, journal.ID, promotion.ID)
	fmt.Println("PASS: demand shift evicted the cooled version to COLD; settings will be restored. Test objects remain in", bucket)
	return nil
}
func (s *smoke) verifyHot(ctx context.Context, version string) error {
	p, err := s.placements.GetPlacement(ctx, &metadatav1.GetPlacementRequest{VersionId: version})
	if err != nil {
		return err
	}
	if p.GetPlacement().GetStorageClass() != "HOT" {
		return fmt.Errorf("placement %s is not HOT", version)
	}
	return nil
}
