package env

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// The latency coefficients are placeholders until stage 10 measures the stand;
// the administrator can change them, and the stored values win over env.
type plannerEnvConfig struct {
	Predictor         string        `env:"PLANNER_PREDICTOR" envDefault:"ewma"`
	ModelRegistry     string        `env:"PLANNER_MODEL_REGISTRY" envDefault:"/models/registry"`
	ModelMaxAge       time.Duration `env:"PLANNER_MODEL_MAX_AGE" envDefault:"72h"`
	ModelUpdateEvery  time.Duration `env:"PLANNER_MODEL_UPDATE_EVERY" envDefault:"24h"`
	ModelMinHistory   time.Duration `env:"PLANNER_MODEL_MIN_HISTORY" envDefault:"26h"`
	ModelTrainWindow  time.Duration `env:"PLANNER_MODEL_TRAIN_WINDOW" envDefault:"168h"`
	ModelTrainTimeout time.Duration `env:"PLANNER_MODEL_TRAIN_TIMEOUT" envDefault:"30m"`
	TrainCommand      string        `env:"PLANNER_TRAIN_COMMAND" envDefault:"python -m sppr_ml train --data {data} --out {out} --params {params}"`
	ModelDir          string        `env:"PLANNER_MODEL_DIR"`
	Enabled           bool          `env:"PLANNER_ENABLED" envDefault:"false"`
	Interval          time.Duration `env:"PLANNER_INTERVAL" envDefault:"300s"`
	KeepPlans         int           `env:"PLANNER_KEEP_PLANS" envDefault:"200"`
	MaxRejectedSaved  int           `env:"PLANNER_MAX_REJECTED" envDefault:"1000"`
	Horizon           time.Duration `env:"PLANNER_HORIZON" envDefault:"1h"`
	HotFixedMs        float64       `env:"PLANNER_HOT_C_MS" envDefault:"1"`
	HotMiBPerMs       float64       `env:"PLANNER_HOT_V_MIB_PER_MS" envDefault:"0.5"`
	ColdFixedMs       float64       `env:"PLANNER_COLD_C_MS" envDefault:"20"`
	ColdMiBPerMs      float64       `env:"PLANNER_COLD_V_MIB_PER_MS" envDefault:"0.05"`
	PenaltyMsPerMiB   float64       `env:"PLANNER_PENALTY_MS_PER_MIB" envDefault:"2"`
	MinGainMs         float64       `env:"PLANNER_MIN_GAIN_MS" envDefault:"100"`
	HotUtilization    float64       `env:"PLANNER_HOT_UTILIZATION" envDefault:"0.9"`
	ColdUtilization   float64       `env:"PLANNER_COLD_UTILIZATION" envDefault:"0.9"`
}

type plannerConfig struct {
	raw plannerEnvConfig
}

func NewPlannerConfig() (*plannerConfig, error) {
	var raw plannerEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	switch {
	case raw.ModelRegistry == "" || raw.TrainCommand == "":
		return nil, fmt.Errorf("PLANNER_MODEL_REGISTRY and PLANNER_TRAIN_COMMAND must not be empty")
	case raw.ModelMaxAge <= 0 || raw.ModelUpdateEvery < time.Second || raw.ModelMinHistory <= 0 || raw.ModelTrainWindow <= 0 || raw.ModelTrainTimeout <= 0:
		return nil, fmt.Errorf("model lifecycle durations must be positive (update interval >= 1s)")
	case raw.Predictor != "ewma" && raw.Predictor != "lgbm":
		return nil, fmt.Errorf("PLANNER_PREDICTOR must be ewma or lgbm")
	case raw.Interval < time.Second:
		return nil, fmt.Errorf("PLANNER_INTERVAL must be at least 1s")
	case raw.KeepPlans < 1:
		return nil, fmt.Errorf("PLANNER_KEEP_PLANS must be at least 1")
	case raw.MaxRejectedSaved < 0:
		return nil, fmt.Errorf("PLANNER_MAX_REJECTED must not be negative")
	case raw.Horizon < time.Second || raw.Horizon%time.Second != 0:
		return nil, fmt.Errorf("PLANNER_HORIZON must be a positive number of whole seconds")
	case raw.HotMiBPerMs <= 0 || raw.ColdMiBPerMs <= 0:
		return nil, fmt.Errorf("PLANNER_HOT_V_MIB_PER_MS and PLANNER_COLD_V_MIB_PER_MS must be positive")
	case raw.HotFixedMs < 0 || raw.ColdFixedMs < 0 || raw.PenaltyMsPerMiB < 0 || raw.MinGainMs < 0:
		return nil, fmt.Errorf("PLANNER_*_C_MS, PLANNER_PENALTY_MS_PER_MIB and PLANNER_MIN_GAIN_MS must not be negative")
	case raw.HotUtilization <= 0 || raw.HotUtilization > 1 || raw.ColdUtilization <= 0 || raw.ColdUtilization > 1:
		return nil, fmt.Errorf("PLANNER_HOT_UTILIZATION and PLANNER_COLD_UTILIZATION must be in (0, 1]")
	}
	return &plannerConfig{raw: raw}, nil
}

func (c *plannerConfig) Enabled() bool            { return c.raw.Enabled }
func (c *plannerConfig) Interval() time.Duration  { return c.raw.Interval }
func (c *plannerConfig) KeepPlans() int           { return c.raw.KeepPlans }
func (c *plannerConfig) MaxRejectedSaved() int    { return c.raw.MaxRejectedSaved }
func (c *plannerConfig) Horizon() time.Duration   { return c.raw.Horizon }
func (c *plannerConfig) HotFixedMs() float64      { return c.raw.HotFixedMs }
func (c *plannerConfig) HotMiBPerMs() float64     { return c.raw.HotMiBPerMs }
func (c *plannerConfig) ColdFixedMs() float64     { return c.raw.ColdFixedMs }
func (c *plannerConfig) ColdMiBPerMs() float64    { return c.raw.ColdMiBPerMs }
func (c *plannerConfig) PenaltyMsPerMiB() float64 { return c.raw.PenaltyMsPerMiB }
func (c *plannerConfig) MinGainMs() float64       { return c.raw.MinGainMs }
func (c *plannerConfig) HotUtilization() float64  { return c.raw.HotUtilization }
func (c *plannerConfig) ColdUtilization() float64 { return c.raw.ColdUtilization }

func (c *plannerConfig) Predictor() string { return c.raw.Predictor }
func (c *plannerConfig) ModelDir() string  { return c.raw.ModelDir }

func (c *plannerConfig) ModelRegistry() string            { return c.raw.ModelRegistry }
func (c *plannerConfig) ModelMaxAge() time.Duration       { return c.raw.ModelMaxAge }
func (c *plannerConfig) ModelUpdateEvery() time.Duration  { return c.raw.ModelUpdateEvery }
func (c *plannerConfig) ModelMinHistory() time.Duration   { return c.raw.ModelMinHistory }
func (c *plannerConfig) ModelTrainWindow() time.Duration  { return c.raw.ModelTrainWindow }
func (c *plannerConfig) ModelTrainTimeout() time.Duration { return c.raw.ModelTrainTimeout }
func (c *plannerConfig) TrainCommand() string             { return c.raw.TrainCommand }
