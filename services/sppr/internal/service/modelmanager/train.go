package modelmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
)

type TrainFunc func(context.Context, string, string, string, string) error

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

type limitedOutput struct {
	mu   sync.Mutex
	data []byte
}

func (w *limitedOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	if len(w.data) < 8192 {
		w.data = append(w.data, p[:min(len(p), 8192-len(w.data))]...)
	}
	return n, nil
}

// RunCommand expands trusted operator configuration, shell-quotes all generated
// values and kills the entire process group on timeout or application shutdown.
func RunCommand(ctx context.Context, command, data, out, params string) error {
	command = strings.NewReplacer("{data}", shellQuote(data), "{out}", shellQuote(out), "{params}", shellQuote(params)).Replace(command)
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = 2 * time.Second
	output := &limitedOutput{}
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("training process: %w (%v): %s", err, ctx.Err(), output.data)
	}
	return nil
}
func (m *Manager) Reserve(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.training {
		return "training_in_progress", nil
	}
	h, err := m.horizon(ctx)
	if err != nil {
		return "", err
	}
	c, err := m.history.Coverage(ctx, m.cfg.Now().Add(-m.cfg.Window-features.MaxWindow))
	if err != nil {
		return "", err
	}
	reason := ""
	switch {
	case h%time.Minute != 0:
		reason = "horizon_not_whole_minutes"
	case !c.Known:
		reason = "unknown_completeness"
	case c.Watermark.Sub(c.Start) < m.cfg.MinHistory:
		reason = "insufficient_history"
	default:
		q := features.Quality(c, c.Watermark.Add(-m.cfg.MinHistory), c.Watermark)
		if !q.Usable {
			reason = q.Reason
		}
	}
	m.historyReason = reason
	if reason != "" {
		return reason, nil
	}
	m.training = true
	m.progress = "queued"
	return "", nil
}
func (m *Manager) Release() {
	m.mu.Lock()
	m.training = false
	m.queued = nil
	m.progress = ""
	m.mu.Unlock()
}

// Queue joins the administrator transaction. A crash before Start leaves a
// durable candidate which Restore marks training_interrupted.
func (m *Manager) Queue(ctx context.Context, source string) error {
	v := model.ModelVersion{ModelVersion: m.failureID(), Status: "candidate", Source: source, CreatedAt: m.cfg.Now().UTC()}
	if err := m.repo.Save(ctx, v); err != nil {
		return err
	}
	m.mu.Lock()
	m.queued = &v
	m.mu.Unlock()
	return nil
}
func (m *Manager) Start(source string) {
	m.mu.Lock()
	if m.queued != nil {
		v := *m.queued
		m.last = &v
	}
	m.mu.Unlock()
	m.queue <- source
}
func (m *Manager) progressTo(s string) { m.mu.Lock(); m.progress = s; m.mu.Unlock() }
func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.Every)
	defer ticker.Stop()
	defer m.Release()
	for {
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case source := <-m.queue:
			m.train(ctx, source)
			m.Release()
		case <-ticker.C:
			m.mu.Lock()
			m.next = m.cfg.Now().Add(m.cfg.Every)
			m.mu.Unlock()
			reason, err := m.Reserve(ctx)
			if err != nil {
				m.mu.Lock()
				m.historyReason = err.Error()
				m.mu.Unlock()
				continue
			}
			if reason == "" {
				m.train(ctx, "scheduled")
				m.Release()
			}
		}
	}
}
func (m *Manager) parameters() (string, error) {
	params := map[string]any{"num_leaves": 15, "learning_rate": 0.03, "min_data_in_leaf": 20, "lambda_l2": 1.0}
	if a := m.current.Load(); a != nil {
		mf, err := manifest(filepath.Join(m.cfg.Registry, a.version.ModelVersion))
		if err != nil {
			return "", err
		}
		var previous map[string]any
		if err := json.Unmarshal(mf.Hyperparameters, &previous); err != nil {
			return "", err
		}
		if _, ok := previous["classifier"]; ok {
			return "", fmt.Errorf("separate classifier/regressor hyperparameters cannot be represented by this trainer")
		}
		for k, v := range previous {
			if k != "rounds" {
				params[k] = v
			}
		}
	}
	params["num_threads"] = 2
	b, err := json.Marshal(params)
	return string(b), err
}
func (m *Manager) train(ctx context.Context, source string) {
	// The entire preparation + subprocess + validation shares the timeout.
	ctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()
	started := m.cfg.Now().UTC()
	v := model.ModelVersion{ModelVersion: m.failureID(), Status: "candidate", Source: source, CreatedAt: started}
	m.mu.Lock()
	queued := m.queued
	m.queued = nil
	m.mu.Unlock()
	if queued != nil {
		v = *queued
	} else {
		if err := m.repo.Save(ctx, v); err != nil {
			m.mu.Lock()
			m.journalError = err.Error()
			m.mu.Unlock()
			return
		}
	}

	fail := func(err error) {
		v.TrainSeconds = max(0, m.cfg.Now().Sub(started).Seconds())
		saveCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		_ = m.reject(saveCtx, v, err)
	}
	m.mu.Lock()
	initial := v
	m.last = &initial
	m.mu.Unlock()
	m.progressTo("preparing_dataset")
	work, err := os.MkdirTemp(m.cfg.Registry, ".training-")
	if err != nil {
		fail(err)
		return
	}
	defer os.RemoveAll(work)
	dataDir := filepath.Join(work, "data")
	if err := os.Mkdir(dataDir, 0750); err != nil {
		fail(err)
		return
	}
	catalog, err := m.placements.ListCatalog(ctx)
	if err != nil {
		fail(err)
		return
	}
	h, err := m.horizon(ctx)
	if err != nil {
		fail(err)
		return
	}
	data, err := dataset.FromHistory(ctx, m.history, Versions(catalog), dataDir, dataset.HistoryOptions{Now: started, Window: m.cfg.Window, Horizon: h})
	if err != nil {
		fail(err)
		return
	}
	v.TrainedFrom = &data.Train.Start
	v.TrainedTo = data.LastLabelAvailableAt
	if v.TrainedTo == nil {
		v.TrainedTo = &data.Validation.End
	}
	v.TrainRows = data.Rows["train"]
	params, err := m.parameters()
	if err != nil {
		fail(err)
		return
	}
	m.progressTo("training")
	out := filepath.Join(work, "out")
	if err := m.cfg.Train(ctx, m.cfg.Command, dataDir, out, params); err != nil {
		fail(err)
		return
	}
	if err := ctx.Err(); err != nil {
		fail(err)
		return
	}
	m.progressTo("validating")
	v.TrainSeconds = max(0, m.cfg.Now().Sub(started).Seconds())
	if err := m.importArtifact(ctx, out, v); err != nil {
		m.mu.Lock()
		m.journalError = err.Error()
		m.mu.Unlock()
	}
}
