package stand

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
)

func command(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	b, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return b, ctx.Err()
		}
		return b, fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, b)
	}
	return b, nil
}

type compose struct{ file, project string }

func (d compose) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "--project-name", d.project, "-f", d.file}, args...)...)
	cmd.Env = append(os.Environ(), "COMPOSE_PROJECT_NAME="+d.project)
	b, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return b, ctx.Err()
	}
	if err != nil {
		return b, fmt.Errorf("compose %v: %w: %s", args, err, b)
	}
	return b, nil
}
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
func setEnv(s map[string]any, key string, value any) {
	env, ok := s["environment"].(map[string]any)
	if !ok {
		env = map[string]any{}
		s["environment"] = env
	}
	env[key] = fmt.Sprint(value)
}

// isolate removes fixed host ports and globally named resources from the resolved
// source model. The generated project exposes only loopback ephemeral endpoints.
func isolate(doc map[string]any, c Config, sc sim.Config, policy string, budget int64, artifacts map[string]string) error {
	delete(doc, "name")
	services := doc["services"].(map[string]any)
	for _, raw := range services {
		s := raw.(map[string]any)
		delete(s, "container_name")
		delete(s, "ports")
		delete(s, "profiles")
		setEnv(s, "LOGGER_ENABLE_OLTP", false)
	}
	for _, kind := range []string{"volumes", "networks"} {
		if resources, ok := doc[kind].(map[string]any); ok {
			for _, raw := range resources {
				if r, ok := raw.(map[string]any); ok {
					delete(r, "name")
					delete(r, "external")
				}
			}
		}
	}
	ports := map[string]int{"gateway": 8080, "metadata": 50052, "sppr": 8086}
	for service, port := range ports {
		s := services[service].(map[string]any)
		s["ports"] = []any{map[string]any{"target": port, "host_ip": "127.0.0.1", "published": "0", "protocol": "tcp"}}
	}
	// The stand measures read latency, not Metadata's request limiter: at the
	// offered load plus SPPR polling the default 100 rps would reject calls.
	setEnv(services["metadata"].(map[string]any), "RATE_LIMITER_LIMIT", 100000)
	gateway := services["gateway"].(map[string]any)
	setEnv(gateway, "ACCESS_EVENTS_ENABLED", policy != "no-extension")
	setEnv(gateway, "ACCESS_HEARTBEAT_INTERVAL", "1s")
	s := services["sppr"].(map[string]any)
	s["cpus"] = 2
	s["mem_limit"] = "2g"
	// Unauthenticated administrative API is confined to this disposable loopback
	// project, so token expiry cannot change a six-hour experiment mid-run.
	for key, v := range map[string]any{"MIGRATION_ENABLED": true, "PLANNER_ENABLED": true, "ADMIN_AUTH_ENABLED": false, "MIGRATION_WORKERS": 1, "MIGRATION_BUDGET_BYTES": budget, "MIGRATION_BUDGET_WINDOW": fmt.Sprintf("%ds", sc.CycleSeconds), "MIGRATION_MIN_INTERVAL": fmt.Sprintf("%ds", sc.MinIntervalSeconds), "PLANNER_INTERVAL": fmt.Sprintf("%ds", sc.CycleSeconds), "PLANNER_KEEP_PLANS": 100000, "PLANNER_MIN_GAIN_MS": sc.MinGainMs, "PLANNER_PENALTY_MS_PER_MIB": sc.PenaltyMs, "PLANNER_HOT_UTILIZATION": sc.Utilization, "PLANNER_COLD_UTILIZATION": sc.Utilization, "PLANNER_HOT_C_MS": 0, "PLANNER_COLD_C_MS": 0, "PLANNER_HOT_V_MIB_PER_MS": 1 / sc.HotReadMs, "PLANNER_COLD_V_MIB_PER_MS": 1 / sc.ColdReadMs} {
		setEnv(s, key, v)
	}
	predictor := policy
	if c.Overhead {
		predictor = "ewma"
	}
	setEnv(s, "PLANNER_PREDICTOR", predictor)
	// Keep the service's own volumes, including the writable model registry.
	volumes, _ := s["volumes"].([]any)
	for key, path := range artifacts {
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		dest := "/artifacts/" + key
		volumes = append(volumes, map[string]any{"type": "bind", "source": abs, "target": dest, "read_only": true})
	}
	s["volumes"] = volumes
	if policy == "lgbm" {
		setEnv(s, "PLANNER_MODEL_DIR", "/artifacts/model")
		// The bootstrap model is fixed for the run; its synthetic label dates lie in
		// the past, so the production age limit would silently select EWMA.
		setEnv(s, "PLANNER_MODEL_MAX_AGE", "87600h")
		setEnv(s, "PLANNER_MODEL_UPDATE_EVERY", "87600h")
	}
	if policy == "age" || policy == "recency" {
		setEnv(s, "PLANNER_BASELINE_MAP", "/artifacts/"+policy)
	}
	return nil
}
func prepareCompose(ctx context.Context, c Config, sc sim.Config, policy string, budget int64, artifacts map[string]string, dir, project string) (compose, error) {
	source, err := filepath.Abs(c.ComposeFile)
	if err != nil {
		return compose{}, err
	}
	// Resolving first retains absolute build/bind paths when the file moves into results.
	b, err := command(ctx, "compose", "-f", source, "--profile", "services", "--profile", "sppr", "config", "--format", "json")
	if err != nil {
		return compose{}, err
	}
	var doc map[string]any
	if err = json.Unmarshal(b, &doc); err != nil {
		return compose{}, err
	}
	if err = isolate(doc, c, sc, policy, budget, artifacts); err != nil {
		return compose{}, err
	}
	file, err := filepath.Abs(filepath.Join(dir, "compose.json"))
	if err != nil {
		return compose{}, err
	}
	if err = writeJSON(file, doc); err != nil {
		return compose{}, err
	}
	return compose{file, project}, nil
}
func (d compose) endpoint(ctx context.Context, service string, port int) (string, error) {
	b, err := d.run(ctx, "port", service, strconv.Itoa(port))
	return strings.TrimSpace(string(b)), err
}
