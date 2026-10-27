package env

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// byteSize accepts a byte count with an optional binary unit: 1073741824, 512MiB, 1GiB.
type byteSize int64

var byteUnits = []struct {
	suffix string
	factor int64
}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}}

func (b *byteSize) UnmarshalText(text []byte) error {
	raw := strings.TrimSpace(string(text))
	factor := int64(1)
	for _, unit := range byteUnits {
		if strings.HasSuffix(raw, unit.suffix) {
			raw, factor = strings.TrimSpace(strings.TrimSuffix(raw, unit.suffix)), unit.factor
			break
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 || n > (1<<63-1)/factor {
		return fmt.Errorf("invalid size %q; use bytes or a KiB/MiB/GiB/TiB suffix", string(text))
	}
	*b = byteSize(n * factor)
	return nil
}

type migrationEnvConfig struct {
	Enabled            bool          `env:"MIGRATION_ENABLED" envDefault:"false"`
	Workers            int           `env:"MIGRATION_WORKERS" envDefault:"2"`
	MaxAttempts        int           `env:"MIGRATION_MAX_ATTEMPTS" envDefault:"5"`
	RetryBackoff       time.Duration `env:"MIGRATION_RETRY_BACKOFF" envDefault:"1s"`
	SourceCleanupDelay time.Duration `env:"MIGRATION_SOURCE_CLEANUP_DELAY" envDefault:"10m"`
	MaxReadDuration    time.Duration `env:"GATEWAY_MAX_READ_DURATION" envDefault:"5m"`
	BudgetBytes        byteSize      `env:"MIGRATION_BUDGET_BYTES" envDefault:"1GiB"`
	BudgetWindow       time.Duration `env:"MIGRATION_BUDGET_WINDOW" envDefault:"1h"`
	MinInterval        time.Duration `env:"MIGRATION_MIN_INTERVAL" envDefault:"1h"`
}

type migrationConfig struct {
	raw migrationEnvConfig
}

// NewMigrationConfig validates the settings even when the executor is disabled,
// so that enabling it later cannot fail on values that were already wrong.
func NewMigrationConfig() (*migrationConfig, error) {
	var raw migrationEnvConfig
	if err := env.Parse(&raw); err != nil {
		return nil, err
	}
	switch {
	case raw.Workers < 1:
		return nil, fmt.Errorf("MIGRATION_WORKERS must be at least 1")
	case raw.MaxAttempts < 1:
		return nil, fmt.Errorf("MIGRATION_MAX_ATTEMPTS must be at least 1")
	case raw.RetryBackoff <= 0:
		return nil, fmt.Errorf("MIGRATION_RETRY_BACKOFF must be positive")
	case raw.MaxReadDuration <= 0:
		return nil, fmt.Errorf("GATEWAY_MAX_READ_DURATION must be positive")
	// A GET that resolved the old placement may still read the source blob.
	case raw.SourceCleanupDelay <= raw.MaxReadDuration:
		return nil, fmt.Errorf("MIGRATION_SOURCE_CLEANUP_DELAY (%s) must exceed GATEWAY_MAX_READ_DURATION (%s)", raw.SourceCleanupDelay, raw.MaxReadDuration)
	case raw.BudgetBytes <= 0:
		return nil, fmt.Errorf("MIGRATION_BUDGET_BYTES must be positive")
	case raw.BudgetWindow < time.Second:
		return nil, fmt.Errorf("MIGRATION_BUDGET_WINDOW must be at least 1s")
	case raw.MinInterval < 0:
		return nil, fmt.Errorf("MIGRATION_MIN_INTERVAL must not be negative")
	}
	return &migrationConfig{raw: raw}, nil
}

func (c *migrationConfig) Enabled() bool                     { return c.raw.Enabled }
func (c *migrationConfig) Workers() int                      { return c.raw.Workers }
func (c *migrationConfig) MaxAttempts() int                  { return c.raw.MaxAttempts }
func (c *migrationConfig) RetryBackoff() time.Duration       { return c.raw.RetryBackoff }
func (c *migrationConfig) SourceCleanupDelay() time.Duration { return c.raw.SourceCleanupDelay }
func (c *migrationConfig) BudgetBytes() int64                { return int64(c.raw.BudgetBytes) }
func (c *migrationConfig) BudgetWindow() time.Duration {
	return c.raw.BudgetWindow.Truncate(time.Second)
}
func (c *migrationConfig) MinInterval() time.Duration { return c.raw.MinInterval.Truncate(time.Second) }
