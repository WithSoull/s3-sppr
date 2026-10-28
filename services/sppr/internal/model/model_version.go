package model

import "time"

// ModelVersion is a durable training attempt, including rejected artifacts.
type ModelVersion struct {
	ModelVersion       string     `json:"model_version" db:"model_version"`
	Status             string     `json:"status" db:"status"`
	Reason             string     `json:"reason" db:"reason"`
	Source             string     `json:"source" db:"source"`
	TrainedFrom        *time.Time `json:"trained_from" db:"trained_from"`
	TrainedTo          *time.Time `json:"trained_to" db:"trained_to"`
	TrainRows          int64      `json:"train_rows" db:"train_rows"`
	TrainSeconds       float64    `json:"train_seconds" db:"train_seconds"`
	ValidationCovK     *float64   `json:"validation_cov_k" db:"validation_cov_k"`
	ValidationEWMACovK *float64   `json:"validation_ewma_cov_k" db:"validation_ewma_cov_k"`
	CreatedAt          time.Time  `json:"created_at" db:"created_at"`
	ActivatedAt        *time.Time `json:"activated_at" db:"activated_at"`
	RetiredAt          *time.Time `json:"retired_at" db:"retired_at"`
}

type ModelLifecycleStatus struct {
	Active         *ModelVersion `json:"active"`
	AgeSeconds     float64       `json:"age_seconds"`
	MaxAgeSeconds  float64       `json:"max_age_seconds"`
	FallbackReason string        `json:"fallback_reason"`
	LastAttempt    *ModelVersion `json:"last_attempt"`
	NextAttempt    time.Time     `json:"next_attempt"`
	Training       bool          `json:"training"`
	Progress       string        `json:"progress"`
	HistoryReason  string        `json:"history_reason"`
	JournalError   string        `json:"journal_error,omitempty"`
}
