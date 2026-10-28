package modelmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/lgbm"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

type testDeps struct {
	repo       *mocks.ModelVersionRepositoryMock
	history    *mocks.HistoryRepositoryMock
	placements *mocks.PlacementClientMock
	rows       map[string]model.ModelVersion
	now        time.Time
}

func deps(t *testing.T) *testDeps {
	mc := minimock.NewController(t)
	d := &testDeps{repo: mocks.NewModelVersionRepositoryMock(mc), history: mocks.NewHistoryRepositoryMock(mc), placements: mocks.NewPlacementClientMock(mc), rows: map[string]model.ModelVersion{}, now: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)}
	d.repo.SaveMock.Optional().Set(func(_ context.Context, v model.ModelVersion) error { d.rows[v.ModelVersion] = v; return nil })
	d.repo.RenameMock.Optional().Set(func(_ context.Context, old, new string) error {
		v := d.rows[old]
		delete(d.rows, old)
		v.ModelVersion = new
		d.rows[new] = v
		return nil
	})
	d.repo.ActivateMock.Optional().Set(func(_ context.Context, id string, at time.Time) error {
		for key, v := range d.rows {
			if v.Status == "active" {
				v.Status = "retired"
				v.Reason = "replaced"
				v.RetiredAt = &at
				d.rows[key] = v
			}
		}
		v := d.rows[id]
		v.Status = "active"
		v.ActivatedAt = &at
		d.rows[id] = v
		return nil
	})
	d.repo.RetireMock.Optional().Set(func(_ context.Context, id, reason string, at time.Time) error {
		v := d.rows[id]
		v.Status = "retired"
		v.Reason = reason
		v.RetiredAt = &at
		d.rows[id] = v
		return nil
	})
	d.repo.ListMock.Optional().Set(func(context.Context, time.Time, int) ([]model.ModelVersion, error) {
		rows := []model.ModelVersion{}
		for _, v := range d.rows {
			rows = append(rows, v)
		}
		return rows, nil
	})
	d.repo.FailInterruptedMock.Optional().Return(nil)
	d.placements.ListCatalogMock.Optional().Return([]model.Placement{{Bucket: "b", Key: "k", VersionID: "v", SizeBytes: 10, CreatedAt: d.now.Add(-72 * time.Hour)}}, nil)
	d.history.CoverageMock.Optional().Return(model.Coverage{Known: true, Start: d.now.Add(-72 * time.Hour), Watermark: d.now}, nil)
	d.history.ReadsSinceMock.Optional().Return(nil, nil)
	return d
}
func (d *testDeps) manager(t *testing.T) *Manager {
	return New(Config{Registry: t.TempDir(), Horizon: time.Hour, MaxAge: 72 * time.Hour, Every: 24 * time.Hour, MinHistory: 26 * time.Hour, Window: 7 * 24 * time.Hour, Timeout: time.Second, Now: func() time.Time { return d.now }}, d.repo, d.history, d.placements)
}
func artifact(t *testing.T, version string, now time.Time) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"manifest.json", "classifier.txt", "regressor.txt", "parity.json"} {
		b, e := os.ReadFile(filepath.Join("..", "predictor", "lgbm", "testdata", name))
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), b, 0600))
	}
	var mf map[string]any
	b, e := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, e)
	require.NoError(t, json.Unmarshal(b, &mf))
	mf["model_version"] = version
	mf["train_interval"] = map[string]any{"start": now.Add(-48 * time.Hour), "end": now.Add(-time.Hour), "rows": 100}
	mf["validation_interval"] = map[string]any{"start": now.Add(-time.Hour), "end": now}
	b, e = json.Marshal(mf)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600))
	return dir
}
func importVersion(t *testing.T, m *Manager, d *testDeps, version string) {
	t.Helper()
	v := model.ModelVersion{ModelVersion: m.failureID(), Status: "candidate", Source: "manual", CreatedAt: d.now}
	require.NoError(t, d.repo.Save(context.Background(), v))
	require.NoError(t, m.importArtifact(context.Background(), artifact(t, version, d.now), v))
}
func TestSwapStalenessAndRestore(t *testing.T) {
	d := deps(t)
	m := d.manager(t)
	importVersion(t, m, d, "A")
	a := m.Capture(context.Background(), time.Hour)
	require.Equal(t, "A", a.ModelVersion())
	importVersion(t, m, d, "B")
	require.Equal(t, "B", m.Capture(context.Background(), time.Hour).ModelVersion())
	require.Equal(t, "A", a.ModelVersion())
	restored := New(m.cfg, d.repo, d.history, d.placements)
	require.NoError(t, restored.Restore(context.Background()))
	require.Equal(t, "B", restored.Capture(context.Background(), time.Hour).ModelVersion())
	d.now = d.now.Add(72 * time.Hour)
	require.Equal(t, "B", m.Capture(context.Background(), time.Hour).ModelVersion())
	d.now = d.now.Add(time.Second)
	require.Equal(t, ewma.MethodVersion, m.Capture(context.Background(), time.Hour).MethodVersion())
	require.Equal(t, "model_stale", d.rows["B"].Reason)
	importVersion(t, m, d, "C")
	require.Equal(t, "C", m.Capture(context.Background(), time.Hour).ModelVersion())
}
func TestCandidateRejectedKeepsActive(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*testing.T, string)
		nan    bool
	}{
		{name: "horizon", change: func(t *testing.T, dir string) { mutate(t, dir, "horizon_seconds", 120) }},
		{name: "feature names", change: func(t *testing.T, dir string) { mutate(t, dir, "feature_names", []string{"wrong"}) }},
		{name: "parity", change: func(t *testing.T, dir string) {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "parity.json"), []byte(`{"rows":[]}`), 0600))
		}},
		{name: "missing regressor", change: func(t *testing.T, dir string) { require.NoError(t, os.Remove(filepath.Join(dir, "regressor.txt"))) }},
		{name: "NaN probe", nan: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := deps(t)
			m := d.manager(t)
			importVersion(t, m, d, "A")
			dir := artifact(t, "B", d.now)
			if tt.change != nil {
				tt.change(t, dir)
			}
			if tt.nan {
				mc := minimock.NewController(t)
				p := mocks.NewPredictorMock(mc)
				p.ModelVersionMock.Return("B")
				p.PredictMock.Return(model.DemandForecast{ExpectedReads: math.NaN()}, nil)
				m.cfg.Loader = func(string, time.Duration) (service.Predictor, error) { return p, nil }
			}
			v := model.ModelVersion{ModelVersion: m.failureID(), Status: "candidate", Source: "manual", CreatedAt: d.now}
			require.NoError(t, d.repo.Save(context.Background(), v))
			require.NoError(t, m.importArtifact(context.Background(), dir, v))
			st := m.Status(context.Background())
			require.Equal(t, "rejected", st.LastAttempt.Status)
			require.NotEmpty(t, st.LastAttempt.Reason)
			require.Equal(t, "A", m.Capture(context.Background(), time.Hour).ModelVersion())
		})
	}
}
func mutate(t *testing.T, dir, key string, value any) {
	t.Helper()
	mf, e := manifest(dir)
	require.NoError(t, e)
	b, e := json.Marshal(mf)
	require.NoError(t, e)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(b, &fields))
	fields[key] = value
	b, e = json.Marshal(fields)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600))
}
func TestBootstrapOnlyEmptyJournal(t *testing.T) {
	d := deps(t)
	m := d.manager(t)
	m.cfg.Bootstrap = artifact(t, "bootstrap", d.now)
	require.NoError(t, m.Restore(context.Background()))
	require.Equal(t, "bootstrap", m.Capture(context.Background(), time.Hour).ModelVersion())
	require.Equal(t, "bootstrap", d.rows["bootstrap"].Source)
}
func TestHistoryAndSingleTrainingGate(t *testing.T) {
	for _, tt := range []struct {
		name   string
		c      model.Coverage
		reason string
	}{{"unknown", model.Coverage{}, "unknown_completeness"}, {"short", model.Coverage{Known: true, Start: time.Now(), Watermark: time.Now().Add(time.Hour)}, "insufficient_history"}} {
		t.Run(tt.name, func(t *testing.T) {
			d := deps(t)
			m := d.manager(t)
			d.history.CoverageMock.Return(tt.c, nil)
			reason, e := m.Reserve(context.Background())
			require.NoError(t, e)
			require.Equal(t, tt.reason, reason)
			require.Empty(t, d.rows)
		})
	}
	d := deps(t)
	m := d.manager(t)
	reason, e := m.Reserve(context.Background())
	require.NoError(t, e)
	require.Empty(t, reason)
	reason, e = m.Reserve(context.Background())
	require.NoError(t, e)
	require.Equal(t, "training_in_progress", reason)
	m.Release()
}
func TestTrainingFailuresAreJournaled(t *testing.T) {
	for _, tt := range []struct {
		name    string
		train   TrainFunc
		want    string
		timeout time.Duration
	}{
		{"error", func(context.Context, string, string, string, string) error { return fmt.Errorf("exit 7") }, "exit 7", 5 * time.Second},
		{"timeout", func(ctx context.Context, _ string, _ string, _ string, _ string) error {
			<-ctx.Done()
			return ctx.Err()
		}, "deadline", 100 * time.Millisecond},
		{"empty output", func(context.Context, string, string, string, string) error { return nil }, "manifest.json", 5 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := deps(t)
			m := d.manager(t)
			importVersion(t, m, d, "A")
			m.cfg.Window = 7 * time.Hour
			m.cfg.Timeout = tt.timeout
			m.cfg.Train = tt.train
			// catalogue is old and coverage supplies all labels; zero-demand data is
			// sufficient to exercise process failures without fitting a fake model.
			m.train(context.Background(), "manual")
			require.Equal(t, "A", m.Capture(context.Background(), time.Hour).ModelVersion())
			st := m.Status(context.Background())
			require.Equal(t, "rejected", st.LastAttempt.Status)
			require.Contains(t, st.LastAttempt.Reason, tt.want)
		})
	}
}
func TestRunCommandTimeoutAndQuoting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, RunCommand(ctx, "sleep 10", "", "", ""))
	out := filepath.Join(t.TempDir(), "data ' $(echo nope)")
	require.NoError(t, RunCommand(context.Background(), "printf %s {params} > {out}", "", out, `{"num_threads":2}`))
	b, e := os.ReadFile(out)
	require.NoError(t, e)
	require.Equal(t, `{"num_threads":2}`, string(b))
}

// Compile-time assertion also catches accidental runtime API drift.
var _ service.ModelLifecycle = (*Manager)(nil)
var _ = lgbm.MethodVersion

func TestTrainingSuccess(t *testing.T) {
	d := deps(t)
	m := d.manager(t)
	m.cfg.Window = 7 * time.Hour
	artifactDir := artifact(t, "trained", d.now)
	invoked := make(chan struct{}, 1)
	m.cfg.Train = func(ctx context.Context, _ string, data, out, params string) error {
		b, e := os.ReadFile(filepath.Join(data, "manifest.json"))
		require.NoError(t, e)
		require.Contains(t, string(b), `"horizon_seconds": 3600`)
		var p map[string]any
		require.NoError(t, json.Unmarshal([]byte(params), &p))
		require.Equal(t, float64(2), p["num_threads"])
		require.Equal(t, float64(15), p["num_leaves"])
		require.NoError(t, os.Mkdir(out, 0700))
		for _, name := range []string{"manifest.json", "classifier.txt", "regressor.txt", "parity.json"} {
			b, e := os.ReadFile(filepath.Join(artifactDir, name))
			require.NoError(t, e)
			require.NoError(t, os.WriteFile(filepath.Join(out, name), b, 0600))
		}
		select {
		case invoked <- struct{}{}:
		default:
		}
		return nil
	}
	m.train(context.Background(), "scheduled")
	select {
	case <-invoked:
	default:
		t.Fatal("training command did not start")
	}
	require.Equal(t, "trained", m.Capture(context.Background(), time.Hour).ModelVersion())
	require.Equal(t, "scheduled", d.rows["trained"].Source)
}

func TestScheduledTrainingAndShutdown(t *testing.T) {
	d := deps(t)
	m := d.manager(t)
	m.cfg.Window = 7 * time.Hour
	m.cfg.Every = 10 * time.Millisecond
	started := make(chan struct{})
	m.cfg.Train = func(ctx context.Context, _ string, _ string, _ string, _ string) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("scheduled command did not start")
	}
	require.True(t, m.Status(context.Background()).Training)
	reason, e := m.Reserve(context.Background())
	require.NoError(t, e)
	require.Equal(t, "training_in_progress", reason)
	cancel()
	require.NoError(t, <-done)
	st := m.Status(context.Background())
	require.False(t, st.Training)
	require.Equal(t, "scheduled", st.LastAttempt.Source)
	require.Equal(t, "rejected", st.LastAttempt.Status)
}

// This optional offline integration exercises Go history -> CSV.gz -> the real
// Python trainer -> four-file export -> Go parity/probe/activation, without DB.
func TestPythonTrainingRoundTrip(t *testing.T) {
	python := os.Getenv("SPPR_TEST_PYTHON")
	if python == "" {
		t.Skip("set SPPR_TEST_PYTHON to the sppr_ml virtualenv interpreter")
	}
	d := deps(t)
	m := d.manager(t)
	m.cfg.Horizon = 2 * time.Minute
	m.cfg.Window = 140 * time.Minute
	m.cfg.Timeout = time.Minute
	var catalog []model.Placement
	var reads []model.MinuteReads
	for i := 0; i < 16; i++ {
		id := fmt.Sprintf("v-%d", i)
		key := fmt.Sprintf("key-%d", i)
		catalog = append(catalog, model.Placement{Bucket: "b", Key: key, VersionID: id, SizeBytes: 10, CreatedAt: d.now.Add(-72 * time.Hour)})
		if i < 4 {
			for minute := 1; minute <= 140; minute++ {
				reads = append(reads, model.MinuteReads{VersionRef: model.VersionRef{Bucket: "b", ObjectKey: key, VersionID: id}, Minute: d.now.Add(-time.Duration(minute) * time.Minute), Reads: 30, Bytes: 300})
			}
		}
	}
	d.placements.ListCatalogMock.Return(catalog, nil)
	d.history.ReadsSinceMock.Return(reads, nil)
	ml, err := filepath.Abs("../../../ml")
	require.NoError(t, err)
	m.cfg.Command = "PYTHONPATH=" + shellQuote(ml) + " " + shellQuote(python) + " -m sppr_ml train --data {data} --out {out} --params {params}"
	m.train(context.Background(), "manual")
	st := m.Status(context.Background())
	require.NotNil(t, st.LastAttempt)
	require.Equal(t, "active", st.LastAttempt.Status, st.LastAttempt.Reason)
	require.NotNil(t, st.Active)
	require.Greater(t, st.Active.TrainRows, int64(0))
	require.NotNil(t, st.Active.ValidationCovK)
	require.NotNil(t, st.Active.ValidationEWMACovK)
	require.Equal(t, lgbm.MethodVersion, m.Capture(context.Background(), 2*time.Minute).MethodVersion())
	require.Greater(t, st.AgeSeconds, float64(0))
}

func TestFailedTrainingRetainsLastAvailableLabel(t *testing.T) {
	d := deps(t)
	m := d.manager(t)
	m.cfg.Horizon = 2 * time.Minute
	m.cfg.Window = 140 * time.Minute
	m.cfg.Timeout = 5 * time.Second
	m.cfg.Train = func(context.Context, string, string, string, string) error { return fmt.Errorf("training failed") }
	m.train(context.Background(), "manual")
	st := m.Status(context.Background())
	require.NotNil(t, st.LastAttempt)
	require.Equal(t, "rejected", st.LastAttempt.Status)
	require.Equal(t, d.now.Add(-3*time.Minute), *st.LastAttempt.TrainedTo)
}
