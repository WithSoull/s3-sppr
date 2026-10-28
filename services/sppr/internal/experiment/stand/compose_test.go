package stand

import (
	"context"
	"encoding/json"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/testhttp"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
	"github.com/stretchr/testify/require"
)

func source() map[string]any {
	services := map[string]any{}
	for _, n := range []string{"gateway", "sppr", "metadata", "postgres-metadata", "storage-hot", "storage-cold"} {
		services[n] = map[string]any{"ports": []any{"8080:8080"}, "container_name": n, "profiles": []any{"services"}, "environment": map[string]any{}}
	}
	services["sppr"].(map[string]any)["build"] = map[string]any{"context": "/repo", "dockerfile": "services/sppr/Dockerfile"}
	return map[string]any{"name": "opens3", "services": services, "volumes": map[string]any{"data": map[string]any{"name": "global-data"}}, "networks": map[string]any{"default": map[string]any{"name": "global-network"}}}
}
func TestComposeIsolationAndPolicy(t *testing.T) {
	for _, policy := range []string{"ewma", "age", "recency", "frequency", "lgbm", "no-extension", "sppr-update"} {
		t.Run(policy, func(t *testing.T) {
			doc := source()
			c := DefaultConfig()
			sc := sim.DefaultConfig()
			c.Overhead = policy == "no-extension" || policy == "sppr-update"
			require.NoError(t, isolate(doc, c, sc, policy, 64<<20, map[string]string{"model": "models", "age": "age.json"}))
			require.NotContains(t, doc, "name")
			s := doc["services"].(map[string]any)
			sppr := s["sppr"].(map[string]any)
			require.Equal(t, 2, sppr["cpus"])
			require.Equal(t, "2g", sppr["mem_limit"])
			env := sppr["environment"].(map[string]any)
			want := policy
			if c.Overhead {
				want = "ewma"
			}
			require.Equal(t, want, env["PLANNER_PREDICTOR"])
			require.Equal(t, "false", env["ADMIN_AUTH_ENABLED"])
			require.Equal(t, "67108864", env["MIGRATION_BUDGET_BYTES"])
			for n, raw := range s {
				r := raw.(map[string]any)
				require.NotContains(t, r, "container_name")
				require.NotContains(t, r, "profiles")
				if n == "gateway" || n == "metadata" || n == "sppr" {
					ports := r["ports"].([]any)
					port := ports[0].(map[string]any)
					require.Equal(t, "127.0.0.1", port["host_ip"])
					require.Equal(t, "0", port["published"])
				} else {
					require.NotContains(t, r, "ports")
				}
			}
			require.Equal(t, policy != "no-extension", s["gateway"].(map[string]any)["environment"].(map[string]any)["ACCESS_EVENTS_ENABLED"] == "true")
			if c.Overhead {
				require.Equal(t, "services/sppr/Dockerfile.stand", sppr["build"].(map[string]any)["dockerfile"])
			}
		})
	}
}
func TestProjectIdentity(t *testing.T) {
	require.Equal(t, projectName("build/run/ewma"), projectName("build/run/ewma"))
	require.NotEqual(t, projectName("build/run/ewma"), projectName("build/run2/ewma"))
	require.NotEqual(t, projectName("build/run/ewma"), projectName("build/run/lgbm"))
}
func TestStandValidation(t *testing.T) {
	sc := sim.DefaultConfig()
	c := DefaultConfig()
	require.NoError(t, c.Validate(sc))
	c.Load.ShiftAtSeconds = []float64{3600, 10800}
	c.Load.Generator.Scenario = "shift"
	require.NoError(t, c.Validate(sc))
	c.Load.ShiftAtSeconds = []float64{10800, 3600}
	require.Error(t, c.Validate(sc))
	c = DefaultConfig()
	c.Overhead = true
	sc.Budgets = []int64{64 << 20}
	require.ErrorContains(t, c.Validate(sc), "training_data")
}
func TestSnapshotPaginationAndJournals(t *testing.T) {
	s := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/migration/actions":
			if r.URL.Query().Get("page_token") == "" {
				json.NewEncoder(w).Encode(map[string]any{"actions": []any{map[string]string{"action_id": "a"}}, "next_page_token": "next"})
			} else {
				json.NewEncoder(w).Encode(map[string]any{"actions": []any{}})
			}
		case "/api/migration/actions/a":
			w.Write([]byte(`{"action_id":"a","events":[{"to_status":"succeeded"}]}`))
		default:
			w.Write([]byte(`{}`))
		}
	}))
	defer s.Close()
	dir := filepath.Join(t.TempDir(), "snapshot")
	require.NoError(t, (api{s.URL, s.Client()}).snapshot(context.Background(), dir))
	b, err := os.ReadFile(filepath.Join(dir, "action-a.json"))
	require.NoError(t, err)
	require.Contains(t, string(b), "succeeded")
	b, err = os.ReadFile(filepath.Join(dir, "actions.json"))
	require.NoError(t, err)
	var pages []any
	require.NoError(t, json.Unmarshal(b, &pages))
	require.Len(t, pages, 2)
}
func TestConfirmationUsesFinishedTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	s := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)
		_ = json.NewEncoder(w).Encode(map[string]any{"latest_plan": map[string]any{"plan_id": "p", "status": "ready", "created_at": time.Now().Add(-time.Hour), "finished_at": time.Now()}})
	}))
	defer s.Close()
	require.ErrorIs(t, (api{s.URL, s.Client()}).confirmations(ctx, time.Minute), context.DeadlineExceeded)
}

func TestOutcomeRetainsLoadConfiguration(t *testing.T) {
	c := DefaultConfig()
	sc := sim.DefaultConfig()
	b, err := json.Marshal(Outcome{Config: sc, StandConfig: c})
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))
	require.Contains(t, decoded, "load_config")
	require.Contains(t, decoded, "stand_config")
	require.Contains(t, decoded, "config")
	require.Nil(t, decoded["violations"])
}
