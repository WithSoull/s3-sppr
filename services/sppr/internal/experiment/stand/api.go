package stand

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/google/uuid"
)

const statusFailureLimit = time.Minute

type api struct {
	base   string
	client *http.Client
}

func (a api) request(ctx context.Context, method, path string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err = io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, res.StatusCode, b)
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func retry(ctx context.Context, f func() error) error {
	for {
		err := f()
		if err == nil {
			return nil
		}
		if e := sleep(ctx, time.Second); e != nil {
			return fmt.Errorf("%w (last: %v)", e, err)
		}
	}
}
func (a api) snapshot(ctx context.Context, dir string) error {
	if err := os.Mkdir(dir, 0755); err != nil {
		return err
	}
	for name, path := range map[string]string{"status": "/api/planner/status", "settings": "/api/planner/settings", "budget": "/api/migration/budget", "migration-settings": "/api/migration/settings"} {
		var v any
		if err := a.request(ctx, "GET", path, nil, &v); err != nil {
			return err
		}
		if err := writeJSON(dir+"/"+name+".json", v); err != nil {
			return err
		}
	}
	for name, path := range map[string]string{"commands": "/api/planner/commands", "plans": "/api/planner/plans", "actions": "/api/migration/actions"} {
		token := ""
		var pages []any
		for {
			var page map[string]any
			if err := a.request(ctx, "GET", path+"?page_size=100&page_token="+url.QueryEscape(token), nil, &page); err != nil {
				return err
			}
			pages = append(pages, page)
			// Action details include transition journals; list pages alone do not.
			if name == "actions" {
				if rows, ok := page["actions"].([]any); ok {
					for _, raw := range rows {
						r := raw.(map[string]any)
						id, _ := r["action_id"].(string)
						if id == "" {
							return fmt.Errorf("missing action_id")
						}
						var detail any
						if err := a.request(ctx, "GET", "/api/migration/actions/"+id, nil, &detail); err != nil {
							return err
						}
						if err := writeJSON(dir+"/action-"+id+".json", detail); err != nil {
							return err
						}
					}
				}
			}
			token, _ = page["next_page_token"].(string)
			if token == "" {
				break
			}
		}
		if err := writeJSON(dir+"/"+name+".json", pages); err != nil {
			return err
		}
	}
	return nil
}

// Confirm only the latest ready plan, five (configured) seconds after calculation.
// Refusals remain in the service journal and fail the run rather than being hidden.
func (a api) confirmations(ctx context.Context, delay time.Duration) error {
	confirmed := map[string]bool{}
	var failingSince time.Time
	for {
		var st struct {
			Latest *struct {
				ID       string     `json:"plan_id"`
				Status   string     `json:"status"`
				Created  time.Time  `json:"created_at"`
				Finished *time.Time `json:"finished_at"`
			} `json:"latest_plan"`
			Fallback string `json:"fallback_reason"`
		}
		// A transient status failure must not end a six-hour run; a persistent one does.
		if err := a.request(ctx, "GET", "/api/planner/status", nil, &st); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if failingSince.IsZero() {
				failingSince = time.Now()
			}
			if time.Since(failingSince) > statusFailureLimit {
				return err
			}
			if err := sleep(ctx, time.Second); err != nil {
				return err
			}
			continue
		}
		failingSince = time.Time{}
		if st.Fallback != "" {
			return fmt.Errorf("predictor fallback: %s", st.Fallback)
		}
		if p := st.Latest; p != nil && p.Status == "ready" && !confirmed[p.ID] && p.Finished != nil && !time.Now().Before(p.Finished.Add(delay)) {
			var cmd struct {
				Status string `json:"status"`
				Code   string `json:"code"`
			}
			if err := a.request(ctx, "POST", "/api/planner/plans/"+p.ID+"/confirm", map[string]any{"command_id": uuid.NewString()}, &cmd); err != nil {
				return err
			}
			if cmd.Status != "accepted" {
				return fmt.Errorf("confirmation %s: %s/%s", p.ID, cmd.Status, cmd.Code)
			}
			confirmed[p.ID] = true
		}
		if err := sleep(ctx, time.Second); err != nil {
			return err
		}
	}
}
