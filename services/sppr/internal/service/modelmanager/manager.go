package modelmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
	"github.com/google/uuid"
)

type Config struct {
	CurrentHorizon                                      func(context.Context) (time.Duration, error)
	Registry, Bootstrap, Command                        string
	Horizon, MaxAge, Every, MinHistory, Window, Timeout time.Duration
	Now                                                 func() time.Time
	// Loader and Train are injectable to exercise rejection and process failures.
	Loader func(string, time.Duration) (service.Predictor, error)
	Train  TrainFunc
}
type active struct {
	horizon   time.Duration
	predictor service.Predictor
	version   model.ModelVersion
}
type Manager struct {
	cfg                                             Config
	repo                                            repository.ModelVersionRepository
	history                                         repository.HistoryRepository
	placements                                      client.PlacementClient
	current                                         atomic.Pointer[active]
	mu                                              sync.Mutex
	training                                        bool
	queued                                          *model.ModelVersion
	progress, historyReason, journalError, fallback string
	last                                            *model.ModelVersion
	next                                            time.Time
	queue                                           chan string
}

func New(cfg Config, repo repository.ModelVersionRepository, history repository.HistoryRepository, placements client.PlacementClient) *Manager {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Loader == nil {
		cfg.Loader = func(dir string, h time.Duration) (service.Predictor, error) { return lgbm.Load(dir, h) }
	}
	if cfg.Train == nil {
		cfg.Train = RunCommand
	}
	return &Manager{cfg: cfg, repo: repo, history: history, placements: placements, next: cfg.Now().Add(cfg.Every), queue: make(chan string, 1), fallback: "no_model"}
}

// Restore never silently falls back to a retired version. A bad active artifact
// is retired with its reason; a restart resumes on EWMA until successful training.
func (m *Manager) Restore(ctx context.Context) error {
	if err := os.MkdirAll(m.cfg.Registry, 0750); err != nil {
		return err
	}
	if err := m.repo.FailInterrupted(ctx); err != nil {
		return err
	}
	rows, err := m.repo.List(ctx, time.Time{}, 100)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		v := rows[0]
		m.last = &v
	}
	before := time.Time{}
	for {
		for _, v := range rows {
			if v.Status == "retired" && m.fallback == "no_model" && (v.Reason == "model_stale" || v.Reason == "horizon_changed") {
				m.fallback = v.Reason
			}
			if v.Status != "active" {
				continue
			}
			p, err := m.validate(ctx, filepath.Join(m.cfg.Registry, v.ModelVersion), v)
			if err != nil {
				if e := m.repo.Retire(ctx, v.ModelVersion, "restore: "+err.Error(), m.cfg.Now()); e != nil {
					return e
				}
				m.fallback = "restore: " + err.Error()
				return nil
			}
			h, err := m.horizon(ctx)
			if err != nil {
				return err
			}
			m.current.Store(&active{predictor: p, version: v, horizon: h})
			m.fallback = ""
			m.Capture(ctx, h)
			return nil
		}
		if len(rows) < 100 {
			break
		}
		before = rows[len(rows)-1].CreatedAt
		rows, err = m.repo.List(ctx, before, 100)
		if err != nil {
			return err
		}
	}
	if m.last == nil && m.cfg.Bootstrap != "" {
		v := model.ModelVersion{ModelVersion: m.failureID(), Status: "candidate", Source: "bootstrap", CreatedAt: m.cfg.Now().UTC()}
		if err := m.repo.Save(ctx, v); err != nil {
			return err
		}
		return m.importArtifact(ctx, m.cfg.Bootstrap, v)
	}
	return nil
}
func (m *Manager) failureID() string {
	return "failed-" + m.cfg.Now().UTC().Format("20060102T150405.000000000") + "-" + uuid.NewString()
}
func safeVersion(v string) bool {
	return v != "" && v != "." && v != ".." && filepath.Base(v) == v && !strings.ContainsAny(v, "/\\")
}
func manifest(dir string) (lgbm.Manifest, error) {
	var v lgbm.Manifest
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if e != nil {
		return v, e
	}
	e = json.Unmarshal(b, &v)
	return v, e
}

// An empty validation interval is the export of `train --fit-only` with fixed
// parameters (the experiment's bootstrap models); the predictor accepts it too.
func metadata(v *model.ModelVersion, mf lgbm.Manifest) error {
	var train, validation struct {
		Start, End time.Time
		Rows       int64
	}
	if err := json.Unmarshal(mf.TrainInterval, &train); err != nil {
		return err
	}
	if err := json.Unmarshal(mf.ValidationInterval, &validation); err != nil {
		return err
	}
	if train.Start.IsZero() || !train.End.After(train.Start) || validation.Start.Before(train.End) || validation.End.Before(validation.Start) || train.Rows < 0 {
		return fmt.Errorf("invalid training/validation intervals")
	}
	v.TrainedFrom = &train.Start
	v.TrainedTo = &validation.End
	v.TrainRows = train.Rows
	var scores map[string]struct {
		Coverage *float64 `json:"coverage"`
	}
	if err := json.Unmarshal(mf.ValidationMetrics, &scores); err != nil {
		return err
	}
	v.ValidationCovK = scores["lgbm"].Coverage
	v.ValidationEWMACovK = scores["ewma"].Coverage
	for _, p := range []*float64{v.ValidationCovK, v.ValidationEWMACovK} {
		if p != nil && (math.IsNaN(*p) || math.IsInf(*p, 0) || *p < 0) {
			return fmt.Errorf("invalid validation metric")
		}
	}
	return nil
}
func (m *Manager) validate(ctx context.Context, dir string, v model.ModelVersion) (service.Predictor, error) {
	if !safeVersion(v.ModelVersion) {
		return nil, fmt.Errorf("unsafe model_version")
	}
	if v.TrainedTo == nil || v.TrainedTo.After(m.cfg.Now()) {
		return nil, fmt.Errorf("missing/future trained_to")
	}
	h, err := m.horizon(ctx)
	if err != nil {
		return nil, err
	}
	p, err := m.cfg.Loader(dir, h)
	if err != nil {
		return nil, err
	}
	if p.ModelVersion() != v.ModelVersion {
		return nil, fmt.Errorf("model version differs from journal")
	}
	rows, err := m.latestFeatures(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no catalogue feature rows to probe")
	}
	for _, row := range rows {
		f, err := p.Predict(row, h)
		if err != nil {
			return nil, fmt.Errorf("probe %s: %w", row.VersionID, err)
		}
		values := []float64{f.ExpectedReads}
		if f.Hurdle != nil {
			values = append(values, f.Hurdle.Probability, f.Hurdle.ConditionalReads, f.Hurdle.EWMAExpectedReads)
		}
		for _, x := range values {
			if math.IsNaN(x) || math.IsInf(x, 0) || x < 0 {
				return nil, fmt.Errorf("probe returned non-finite or negative prediction")
			}
		}
	}
	return p, nil
}
func (m *Manager) latestFeatures(ctx context.Context) ([]model.FeatureSnapshot, error) {
	catalog, err := m.placements.ListCatalog(ctx)
	if err != nil {
		return nil, err
	}
	at := m.cfg.Now().UTC().Truncate(time.Minute)
	since := at.Add(-features.MaxWindow)
	c, err := m.history.Coverage(ctx, since)
	if err != nil {
		return nil, err
	}
	if c.Known && c.Watermark.Before(at) {
		// Like the planner, probe at W: events after it are not yet complete.
		at = c.Watermark.UTC()
		since = at.Add(-features.MaxWindow)
	}
	reads, err := m.history.ReadsSince(ctx, since, at)
	if err != nil {
		return nil, err
	}
	versions := Versions(catalog)
	return features.NewService().Build(ctx, model.FeatureRequest{At: at, Catalog: versions, Reads: reads, Coverage: c})
}
func Versions(catalog []model.Placement) []model.Version {
	versions := make([]model.Version, 0, len(catalog))
	for _, p := range catalog {
		versions = append(versions, model.Version{VersionRef: model.VersionRef{Bucket: p.Bucket, ObjectKey: p.Key, VersionID: p.VersionID}, SizeBytes: p.SizeBytes, CreatedAt: p.CreatedAt})
	}
	return versions
}
func (m *Manager) reject(ctx context.Context, v model.ModelVersion, reason error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	v.Status = "rejected"
	v.Reason = reason.Error()
	err := m.repo.Save(ctx, v)
	m.mu.Lock()
	m.last = &v
	if err != nil {
		m.journalError = err.Error()
	}
	m.mu.Unlock()
	return err
}
func (m *Manager) importArtifact(ctx context.Context, dir string, v model.ModelVersion) error {
	mf, err := manifest(dir)
	if err != nil {
		return m.reject(ctx, v, err)
	}
	if !safeVersion(mf.ModelVersion) {
		return m.reject(ctx, v, fmt.Errorf("unsafe model_version"))
	}
	if err := metadata(&v, mf); err != nil {
		return m.reject(ctx, v, err)
	}
	target := filepath.Join(m.cfg.Registry, mf.ModelVersion)
	// Never overwrite an existing registry directory, including a rejected model.
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		return m.reject(ctx, v, fmt.Errorf("registry version already exists or cannot be inspected: %s", mf.ModelVersion))
	}
	tmp, err := os.MkdirTemp(m.cfg.Registry, ".import-")
	if err != nil {
		return m.reject(ctx, v, err)
	}
	defer os.RemoveAll(tmp)
	for _, name := range []string{"manifest.json", "classifier.txt", "regressor.txt", "parity.json"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return m.reject(ctx, v, err)
		}
		if err := os.WriteFile(filepath.Join(tmp, name), b, 0640); err != nil {
			return m.reject(ctx, v, err)
		}
	}
	if err := os.Rename(tmp, target); err != nil {
		return m.reject(ctx, v, err)
	}
	if err := m.repo.Rename(ctx, v.ModelVersion, mf.ModelVersion); err != nil {
		return m.reject(ctx, v, err)
	}
	v.ModelVersion = mf.ModelVersion
	if err := m.repo.Save(ctx, v); err != nil {
		return err
	}
	p, err := m.validate(ctx, target, v)
	if err != nil {
		return m.reject(ctx, v, err)
	}
	if m.cfg.Now().Sub(*v.TrainedTo) > m.cfg.MaxAge {
		return m.reject(ctx, v, fmt.Errorf("model_stale"))
	}
	h, err := m.horizon(ctx)
	if err != nil {
		return m.reject(ctx, v, err)
	}
	artifactHorizon := time.Duration(mf.HorizonSeconds) * time.Second
	if h != artifactHorizon {
		return m.reject(ctx, v, fmt.Errorf("planner horizon changed during validation"))
	}
	m.mu.Lock()
	now := m.cfg.Now().UTC()
	if err := m.repo.Activate(ctx, v.ModelVersion, now); err != nil {
		m.mu.Unlock()
		return m.reject(ctx, v, err)
	}
	v.Status = "active"
	v.ActivatedAt = &now
	m.last = &v
	m.fallback = ""
	m.journalError = ""
	m.current.Store(&active{predictor: p, version: v, horizon: artifactHorizon})
	m.mu.Unlock()
	return nil
}

// Capture returns an immutable predictor for the entire planning cycle.
func (m *Manager) Capture(ctx context.Context, h time.Duration) service.Predictor {
	m.mu.Lock()
	defer m.mu.Unlock()
	a := m.current.Load()
	if a == nil {
		return ewma.NewPredictor()
	}
	reason := ""
	if h != a.horizon {
		reason = "horizon_changed"
	} else if a.version.TrainedTo == nil || m.cfg.Now().Sub(*a.version.TrainedTo) > m.cfg.MaxAge {
		reason = "model_stale"
	}
	if reason != "" {
		if err := m.repo.Retire(ctx, a.version.ModelVersion, reason, m.cfg.Now().UTC()); err != nil {
			m.journalError = err.Error()
			m.fallback = reason
			return ewma.NewPredictor()
		}
		m.journalError = ""
		if m.last != nil && m.last.ModelVersion == a.version.ModelVersion {
			v := a.version
			v.Status = "retired"
			v.Reason = reason
			at := m.cfg.Now().UTC()
			v.RetiredAt = &at
			m.last = &v
		}
		m.current.Store(nil)
		m.fallback = reason
		return ewma.NewPredictor()
	}
	return a.predictor
}
func (m *Manager) Status(ctx context.Context) model.ModelLifecycleStatus {
	h, err := m.horizon(ctx)
	if err == nil {
		m.Capture(ctx, h)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := model.ModelLifecycleStatus{MaxAgeSeconds: m.cfg.MaxAge.Seconds(), FallbackReason: m.fallback, LastAttempt: m.last, NextAttempt: m.next, Training: m.training, Progress: m.progress, HistoryReason: m.historyReason, JournalError: m.journalError}
	if a := m.current.Load(); a != nil && m.fallback == "" {
		v := a.version
		st.Active = &v
		st.AgeSeconds = max(0, m.cfg.Now().Sub(*v.TrainedTo).Seconds())
	}
	return st
}
func (m *Manager) Versions(ctx context.Context, before time.Time) ([]model.ModelVersion, error) {
	return m.repo.List(ctx, before, 100)
}

func (m *Manager) horizon(ctx context.Context) (time.Duration, error) {
	if m.cfg.CurrentHorizon != nil {
		return m.cfg.CurrentHorizon(ctx)
	}
	return m.cfg.Horizon, nil
}
