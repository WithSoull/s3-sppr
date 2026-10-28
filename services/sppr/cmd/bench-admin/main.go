// Command bench-admin measures the running administrator API without confirming migrations.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

type plan struct {
	ID       string     `json:"plan_id"`
	Revision int64      `json:"revision"`
	Status   string     `json:"status"`
	Created  time.Time  `json:"created_at"`
	Finished *time.Time `json:"finished_at"`
}
type status struct {
	Latest *plan `json:"latest_plan"`
	Paused *bool `json:"paused"`
}
type statistics struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	Max   float64 `json:"max"`
}

// percentile uses nearest rank (ceil(p*N)); it never changes the caller's sample order.
func percentile(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	i := max(0, min(len(sorted)-1, int(math.Ceil(p*float64(len(sorted))))-1))
	return sorted[i]
}
func summarize(samples []float64) *statistics {
	if len(samples) == 0 {
		return nil
	}
	return &statistics{len(samples), percentile(samples, .5), percentile(samples, .95), percentile(samples, 1)}
}

type client struct {
	api, token string
	http       *http.Client
}

func (c client) request(ctx context.Context, method, path string, body, out any, want int) error {
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.api+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	// Include the complete response body in latency, including recommendation explanations.
	raw, err = io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode != want {
		return fmt.Errorf("%s %s: HTTP %d, expected %d", method, path, res.StatusCode, want)
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// recommendationPlan prefers the latest ready plan; stands that confirm
// automatically keep only confirmed plans, whose pages are served the same way.
func recommendationPlan(plans []plan) *plan {
	var best *plan
	for i := range plans {
		p := &plans[i]
		switch {
		case best == nil:
			best = p
		case (p.Status == "ready") != (best.Status == "ready"):
			if p.Status == "ready" {
				best = p
			}
		case p.Revision > best.Revision:
			best = p
		}
	}
	return best
}

func (c client) plans(ctx context.Context) ([]plan, error) {
	var all []plan
	token := ""
	seen := map[string]bool{}
	for {
		var page struct {
			Plans []plan `json:"plans"`
			Next  string `json:"next_page_token"`
		}
		if err := c.request(ctx, "GET", "/api/planner/plans?page_size=100&page_token="+url.QueryEscape(token), nil, &page, 200); err != nil {
			return nil, err
		}
		all = append(all, page.Plans...)
		if page.Next == "" {
			return all, nil
		}
		if seen[page.Next] {
			return nil, fmt.Errorf("repeated plan page token")
		}
		seen[page.Next], token = true, page.Next
	}
}
func waitUntil(ctx context.Context, at time.Time) error {
	timer := time.NewTimer(time.Until(at))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func sample(ctx context.Context, n int, rate float64, f func() error) ([]float64, error) {
	samples := make([]float64, 0, n)
	next := time.Now()
	for i := 0; i < n; i++ {
		if err := waitUntil(ctx, next); err != nil {
			return nil, err
		}
		started := time.Now()
		// Serial requests; a slow response lowers achieved rate, never causes a catch-up burst.
		next = started.Add(time.Duration(float64(time.Second) / rate))
		if err := f(); err != nil {
			return nil, fmt.Errorf("sample %d: %w", i+1, err)
		}
		samples = append(samples, time.Since(started).Seconds()*1000)
	}
	return samples, nil
}
func (c client) recovery(ctx context.Context, id string, interval time.Duration) error {
	if id == "" || interval <= 0 {
		return fmt.Errorf("recovery needs a saved plan ID and positive poll interval")
	}
	var last error
	for {
		last = c.request(ctx, "GET", "/readyz", nil, nil, 200)
		if last == nil {
			var st status
			last = c.request(ctx, "GET", "/api/planner/status", nil, &st, 200)
			if last == nil {
				if st.Latest != nil && st.Latest.ID == id {
					return nil
				}
				last = fmt.Errorf("latest plan differs from saved %s", id)
			}
		}
		if err := waitUntil(ctx, time.Now().Add(interval)); err != nil {
			return fmt.Errorf("recovery: %w (last probe: %v)", err, last)
		}
	}
}

type result struct {
	Schema             string      `json:"schema"`
	Started            time.Time   `json:"started_at"`
	API                string      `json:"api"`
	Requests           int         `json:"requests"`
	Rate               float64     `json:"rate_per_second"`
	RecommendationPlan string      `json:"recommendation_plan_id"`
	RecommendationFrom string      `json:"recommendation_plan_status"`
	Recommendations    *statistics `json:"recommendations_ms"`
	Command            string      `json:"command"`
	Paused             bool        `json:"original_paused"`
	Acceptance         *statistics `json:"command_acceptance_ms"`
	Plans              []plan      `json:"stored_plans"`
	PlanDurations      *statistics `json:"plan_duration_ms"`
	Unfinished         int         `json:"unfinished_plans"`
	RecoveryPlan       string      `json:"recovery_plan_id,omitempty"`
	RecoverySeconds    *float64    `json:"recovery_seconds"`
	Percentile         string      `json:"percentile_method"`
}

func measure(ctx context.Context, c client, n int, rate float64, restart string, recoveryTimeout time.Duration) (result, error) {
	r := result{Schema: "bench-admin-v1", Started: time.Now().UTC(), API: c.api, Requests: n, Rate: rate, Percentile: "nearest rank: ceil(p*N)"}
	if n <= 0 || rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || rate > float64(time.Second) || float64(time.Second)/rate >= float64(math.MaxInt64) || recoveryTimeout <= 0 {
		return r, fmt.Errorf("requests, rate (<=1e9/s) and recovery timeout must be positive and finite")
	}
	plans, err := c.plans(ctx)
	if err != nil {
		return r, err
	}
	r.Plans = plans
	var durations []float64
	for _, p := range plans {
		if p.Finished == nil {
			r.Unfinished++
			continue
		}
		if p.Created.IsZero() || p.Finished.Before(p.Created) {
			return r, fmt.Errorf("invalid timestamps for plan %s", p.ID)
		}
		durations = append(durations, p.Finished.Sub(p.Created).Seconds()*1000)
	}
	r.PlanDurations = summarize(durations)
	best := recommendationPlan(plans)
	if best == nil {
		return r, fmt.Errorf("no stored plans")
	}
	r.RecommendationPlan, r.RecommendationFrom = best.ID, best.Status
	recs, err := sample(ctx, n, rate, func() error {
		return c.request(ctx, "GET", "/api/planner/plans/"+url.PathEscape(r.RecommendationPlan)+"/recommendations?page_size=100", nil, nil, 200)
	})
	if err != nil {
		return r, err
	}
	r.Recommendations = summarize(recs)
	var st status
	if err = c.request(ctx, "GET", "/api/planner/status", nil, &st, 200); err != nil {
		return r, err
	}
	if st.Paused == nil {
		return r, fmt.Errorf("status missing paused")
	}
	r.Paused = *st.Paused
	r.Command = "resume"
	if r.Paused {
		r.Command = "pause"
	}
	cmds, err := sample(ctx, n, rate, func() error {
		id := uuid.NewString()
		var accepted struct {
			ID     string `json:"command_id"`
			Status string `json:"status"`
		}
		if err := c.request(ctx, "POST", "/api/planner/commands/"+r.Command, map[string]string{"command_id": id}, &accepted, 202); err != nil {
			return err
		}
		if accepted.ID != id || accepted.Status != "accepted" {
			return fmt.Errorf("command was not accepted with its submitted ID")
		}
		return nil
	})
	if err != nil {
		return r, err
	}
	r.Acceptance = summarize(cmds)
	if restart != "" {
		st = status{}
		if err = c.request(ctx, "GET", "/api/planner/status", nil, &st, 200); err != nil {
			return r, err
		}
		if st.Latest == nil || st.Latest.ID == "" {
			return r, fmt.Errorf("status has no latest plan for recovery")
		}
		r.RecoveryPlan = st.Latest.ID
		restartCtx, cancel := context.WithTimeout(ctx, recoveryTimeout)
		defer cancel()
		started := time.Now()
		cmd := exec.CommandContext(restartCtx, "sh", "-c", restart)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		// Bound inherited pipes when a shell launches descendants.
		cmd.WaitDelay = time.Second
		if err = cmd.Run(); err != nil {
			return r, fmt.Errorf("restart command: %w", err)
		}
		if err = c.recovery(restartCtx, r.RecoveryPlan, 250*time.Millisecond); err != nil {
			return r, err
		}
		seconds := time.Since(started).Seconds()
		r.RecoverySeconds = &seconds
	}
	return r, nil
}
func run(args []string) error {
	f := flag.NewFlagSet("bench-admin", flag.ContinueOnError)
	api := f.String("api", "http://localhost:8086", "running SPPR base URL")
	token := f.String("token", "", "administrator Bearer token; omit only when auth is disabled")
	n := f.Int("requests", 100, "requests per latency series")
	rate := f.Float64("rate", 10, "maximum serial requests per second, no catch-up bursts")
	restart := f.String("restart-command", "", "optional shell command (sh -c); use a dedicated stand")
	out := f.String("out", "build/bench-admin.json", "new JSON output file")
	timeout := f.Duration("recovery-timeout", 2*time.Minute, "restart plus recovery deadline")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if _, err := os.Stat(*out); !os.IsNotExist(err) {
		return fmt.Errorf("output exists or cannot be inspected: %s", *out)
	}
	u, err := url.Parse(*api)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return fmt.Errorf("-api must be an HTTP(S) base URL without credentials, query or fragment")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	c := client{strings.TrimRight(*api, "/"), *token, &http.Client{Timeout: 10 * time.Second}}
	defer c.http.CloseIdleConnections()
	r, err := measure(ctx, c, *n, *rate, *restart, *timeout)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(*out), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	err = enc.Encode(r)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
