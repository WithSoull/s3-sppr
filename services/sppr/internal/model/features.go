package model

import "time"

const FeatureRulesVersion = "features-v1"

type VersionRef struct {
	Bucket    string `json:"bucket"`
	ObjectKey string `json:"object_key"`
	VersionID string `json:"version_id"`
}

// Version describes catalogue history, not just today's surviving objects.
type Version struct {
	VersionRef
	SizeBytes int64      `json:"size_bytes"`
	CreatedAt time.Time  `json:"created_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

func (v Version) Active(at time.Time) bool {
	return !v.CreatedAt.After(at) && (v.DeletedAt == nil || v.DeletedAt.After(at))
}

// Coverage must come from a completeness protocol, never MAX(occurred_at).
// Intervals, including gaps, are half-open.
type Interval struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type Coverage struct {
	Known     bool       `json:"known"`
	Start     time.Time  `json:"start"`
	Watermark time.Time  `json:"watermark"`
	Gaps      []Interval `json:"gaps,omitempty"`
}

type DataQuality struct {
	Usable bool   `json:"usable"`
	Reason string `json:"reason"`
}

type Features struct {
	LogSize              float64  `json:"log_size"`
	AgeSeconds           float64  `json:"age_seconds"`
	Overwrites24h        int64    `json:"overwrites_24h"`
	LastReadSeconds      *float64 `json:"last_read_seconds"`
	PreviousReadSeconds  *float64 `json:"previous_read_seconds"`
	Reads5m              int64    `json:"reads_5m"`
	Reads1h              int64    `json:"reads_1h"`
	Reads6h              int64    `json:"reads_6h"`
	Reads24h             int64    `json:"reads_24h"`
	EWMA1h               float64  `json:"ewma_1h"`
	EWMA6h               float64  `json:"ewma_6h"`
	ShortLongRatio       float64  `json:"short_long_ratio"`
	ReadsDayAgo          int64    `json:"reads_day_ago"`
	MeanReadFraction     float64  `json:"mean_read_fraction"`
	BucketReads1h        int64    `json:"bucket_reads_1h"`
	BucketActiveFraction float64  `json:"bucket_active_fraction"`
	ObservedSeconds      float64  `json:"observed_seconds"`
}

type FeatureSnapshot struct {
	VersionRef
	At           time.Time   `json:"at"`
	SizeBytes    int64       `json:"size_bytes"`
	RulesVersion string      `json:"rules_version"`
	Features     Features    `json:"features"`
	Quality      DataQuality `json:"quality"`
}

type FeatureRequest struct {
	At      time.Time
	Catalog []Version
	// Events must already be deduplicated by Ingestor. Future events are ignored.
	Events []AccessEvent
	// Reads is the Ingestor's per-minute aggregate, an alternative to Events
	// that keeps a day of demand small enough for the planner's memory limit.
	Reads    []MinuteReads
	Coverage Coverage
}

type HurdleForecast struct {
	Probability       float64 `json:"probability"`
	ConditionalReads  float64 `json:"conditional_reads"`
	EWMAExpectedReads float64 `json:"ewma_expected_reads"`
}

// DemandForecast is shared by EWMA and the ML predictor.
type DemandForecast struct {
	VersionRef
	At                  time.Time       `json:"at"`
	HorizonSeconds      int64           `json:"horizon_seconds"`
	ExpectedReads       float64         `json:"expected_reads"`
	Quality             DataQuality     `json:"quality"`
	MethodVersion       string          `json:"method_version"`
	ModelVersion        string          `json:"model_version,omitempty"`
	Hurdle              *HurdleForecast `json:"hurdle,omitempty"`
	FeatureRulesVersion string          `json:"feature_rules_version"`
}

// MinuteReads is one row of the Ingestor's per-minute demand aggregate.
type MinuteReads struct {
	VersionRef
	Minute time.Time
	Reads  int64
	Bytes  int64
}
