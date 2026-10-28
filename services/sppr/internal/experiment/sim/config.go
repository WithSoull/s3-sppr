// Package sim executes logical-time placement experiments using the production
// feature builder, predictors and planner. It has no database or network path.
package sim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

const Version = "placement-sim-v1"

var Policies = []string{"age", "recency", "frequency", "ewma", "lgbm"}

type Config struct {
	Generator            synthetic.Config `json:"generator"`
	Budgets              []int64          `json:"budgets_bytes"`
	Policies             []string         `json:"policies"`
	HotBytes             int64            `json:"hot_bytes"`
	ColdBytes            int64            `json:"cold_bytes"`
	Utilization          float64          `json:"utilization"`
	CycleSeconds         int              `json:"cycle_seconds"`
	ConfirmSeconds       int              `json:"confirm_seconds"`
	MinIntervalSeconds   int              `json:"min_interval_seconds"`
	Slots                int              `json:"slots"`
	MigrationParallelism int              `json:"migration_parallelism"`
	HotReadMs            float64          `json:"hot_read_ms_per_mib"`
	ColdReadMs           float64          `json:"cold_read_ms_per_mib"`
	HotWriteMs           float64          `json:"hot_write_ms_per_mib"`
	ColdWriteMs          float64          `json:"cold_write_ms_per_mib"`
	MinGainMs            float64          `json:"min_gain_ms"`
	PenaltyMs            float64          `json:"penalty_ms_per_mib"`
	ReadFraction         float64          `json:"read_fraction"`
	Sizes                []int64          `json:"object_sizes_bytes,omitempty"`
	UpdateModel          bool             `json:"update_model_24h"`
	Params               json.RawMessage  `json:"params"`
	Python               string           `json:"python"`
	MLDir                string           `json:"ml_dir"`
	RawLatencies         bool             `json:"raw_latencies"`
}

func DefaultConfig() Config {
	return Config{Generator: synthetic.Config{Scenario: "stable", Start: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), Objects: 10000, SizeBytes: model.MiB, ReadsPerSecond: 20, SnapshotSeconds: 300, HotFraction: .1, HotProbability: .8, DecayHours: 24, PeriodHours: 24, BurstAtHours: 100, BurstDurationHours: .5, ShiftEveryHours: 24}, Budgets: []int64{64 * model.MiB, 256 * model.MiB, 512 * model.MiB}, Policies: append([]string(nil), Policies...), HotBytes: 1 << 30, ColdBytes: 12 << 30, Utilization: .9, CycleSeconds: 300, ConfirmSeconds: 5, MinIntervalSeconds: 3600, Slots: 8, MigrationParallelism: 1, HotReadMs: 10, ColdReadMs: 40, HotWriteMs: 10, ColdWriteMs: 40, ReadFraction: 1, UpdateModel: true, Params: json.RawMessage(`{"num_leaves":15,"learning_rate":0.1,"min_data_in_leaf":200,"lambda_l2":1.0}`), Python: "services/sppr/ml/.venv/bin/python", MLDir: "services/sppr/ml"}
}
func ReadConfig(path string) (Config, error) {
	c := DefaultConfig()
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	err = dec.Decode(&c)
	if err == nil && dec.Decode(new(any)) != io.EOF {
		err = fmt.Errorf("config must contain one JSON object")
	}
	if err != nil {
		return c, err
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if err := c.Generator.Validate(); err != nil {
		return err
	}
	if !c.Generator.Start.Equal(c.Generator.Start.Truncate(time.Minute)) {
		return fmt.Errorf("generator start must be minute aligned")
	}
	if c.Generator.Lifecycle {
		return fmt.Errorf("sim v1 requires a fixed catalogue (lifecycle=false)")
	}
	if c.HotBytes <= 0 || c.ColdBytes <= 0 || c.Utilization <= 0 || c.Utilization > 1 || c.CycleSeconds < 60 || c.CycleSeconds%60 != 0 || 3600%c.CycleSeconds != 0 || c.ConfirmSeconds < 0 || c.MinIntervalSeconds < 0 || c.Slots < 1 || c.MigrationParallelism < 1 || c.MigrationParallelism > c.Slots || c.ReadFraction <= 0 || c.ReadFraction > 1 {
		return fmt.Errorf("invalid simulator constraints")
	}
	for _, v := range []float64{c.HotReadMs, c.ColdReadMs, c.HotWriteMs, c.ColdWriteMs, c.Utilization, c.ReadFraction} {
		if !finite(v) || v <= 0 {
			return fmt.Errorf("invalid time/fraction")
		}
	}
	if !finite(c.MinGainMs) || !finite(c.PenaltyMs) || c.MinGainMs < 0 || c.PenaltyMs < 0 {
		return fmt.Errorf("invalid planner penalty/threshold")
	}
	if len(c.Budgets) == 0 || len(c.Policies) == 0 {
		return fmt.Errorf("empty matrix")
	}
	seen := map[string]bool{}
	for _, p := range c.Policies {
		if !slices.Contains(Policies, p) || seen[p] {
			return fmt.Errorf("invalid or duplicate policy %q", p)
		}
		seen[p] = true
	}
	bs := map[int64]bool{}
	for _, b := range c.Budgets {
		if b <= 0 || bs[b] {
			return fmt.Errorf("invalid or duplicate budget")
		}
		bs[b] = true
	}
	for _, s := range c.Sizes {
		if s <= 0 {
			return fmt.Errorf("invalid object size")
		}
	}
	var total int64
	for i := 0; i < c.Generator.Objects; i++ {
		size := c.Generator.SizeBytes
		if len(c.Sizes) > 0 {
			size = c.Sizes[i%len(c.Sizes)]
		}
		if size > c.ColdBytes-total {
			return fmt.Errorf("initial catalogue exceeds COLD capacity")
		}
		total += size
	}
	if total > int64(float64(c.ColdBytes)*c.Utilization) {
		return fmt.Errorf("initial catalogue exceeds COLD utilization")
	}
	var params map[string]any
	if json.Unmarshal(c.Params, &params) != nil || len(params) == 0 {
		return fmt.Errorf("explicit fixed params required")
	}
	if c.Python == "" || c.MLDir == "" {
		return fmt.Errorf("python and ml_dir required")
	}
	return nil
}
func (c Config) params() model.PlannerParams {
	return model.PlannerParams{Horizon: time.Hour, HotMiBPerMs: 1 / c.HotReadMs, ColdMiBPerMs: 1 / c.ColdReadMs, HotUtilization: c.Utilization, ColdUtilization: c.Utilization, MinGainMs: c.MinGainMs, PenaltyMsPerMiB: c.PenaltyMs, MaxRejectedSaved: 0}
}
