package lgbm

import (
	"bufio"
	"bytes"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dmitryikh/leaves"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	cls := readTestdata(t, "classifier.txt")
	reg := readTestdata(t, "regressor.txt")
	cases := []struct {
		name       string
		classifier string
		regressor  string
		wantErr    string
	}{
		{name: "lightgbm 4 files", classifier: cls, regressor: reg},
		{name: "already v3", classifier: strings.Replace(cls, "version=v4", "version=v3", 1), regressor: reg},
		{name: "swapped models", classifier: reg, regressor: cls, wantErr: `objective "poisson", want "binary sigmoid:1"`},
		{name: "scaled sigmoid", classifier: strings.Replace(cls, "binary sigmoid:1", "binary sigmoid:2", 1), regressor: reg, wantErr: "objective"},
		{name: "feature order", classifier: strings.Replace(cls, "reads_5m reads_1h", "reads_1h reads_5m", 1), regressor: reg, wantErr: "feature_names"},
		{name: "linear tree", classifier: cls, regressor: strings.Replace(reg, "is_linear=0", "is_linear=1", 1), wantErr: "linear trees"},
		{name: "feature dimension", classifier: strings.Replace(cls, "max_feature_idx=16", "max_feature_idx=17", 1), regressor: reg, wantErr: "model dimensions"},
		{name: "random forest", classifier: strings.Replace(cls, "version=v4", "version=v4\naverage_output", 1), regressor: reg, wantErr: "random forest"},
		{name: "malformed tree array", classifier: regexp.MustCompile(`(?m)^decision_type=.*$`).ReplaceAllString(cls, "decision_type=2"), regressor: reg, wantErr: "invalid LightGBM text"},
		{name: "unknown version", classifier: strings.Replace(cls, "version=v4", "version=v5", 1), regressor: reg, wantErr: "unsupported model version"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			c, r := filepath.Join(dir, "c.txt"), filepath.Join(dir, "r.txt")
			require.NoError(t, os.WriteFile(c, []byte(tt.classifier), 0o600))
			require.NoError(t, os.WriteFile(r, []byte(tt.regressor), 0o600))
			m, err := loadPair(t, c, r)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, m)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, m)
		})
	}
}

// Records the behaviour of the pinned leaves version that the explicit transforms rely on.
func TestLeavesBehaviour(t *testing.T) {
	cls := readTestdata(t, "classifier.txt")
	reg := readTestdata(t, "regressor.txt")

	_, err := leaves.LGEnsembleFromReader(bufio.NewReader(strings.NewReader(cls)), false)
	require.ErrorContains(t, err, "only version=v3 is supported")

	load := func(s string, transform bool) *leaves.Ensemble {
		e, err := leaves.LGEnsembleFromReader(bufio.NewReader(strings.NewReader(strings.Replace(s, "version=v4", "version=v3", 1))), transform)
		require.NoError(t, err)
		return e
	}
	rawCls, ownCls := load(cls, false), load(cls, true)
	rawReg, ownReg := load(reg, false), load(reg, true)

	x := make([]float64, len(featureNames))
	x[3], x[4] = math.NaN(), math.NaN()
	m, err := Load("testdata", time.Hour)
	require.NoError(t, err)
	got, err := m.predictRaw(x)
	require.NoError(t, err)

	require.Equal(t, got.ClassifierRaw, rawCls.PredictSingle(x, 0))
	require.Equal(t, got.RegressorRaw, rawReg.PredictSingle(x, 0))
	require.InDelta(t, got.Probability, ownCls.PredictSingle(x, 0), 1e-15)
	require.InDelta(t, got.ExcessReads, ownReg.PredictSingle(x, 0), 1e-15*got.ExcessReads)
	require.NotEqual(t, got.Probability, got.ClassifierRaw)
	require.NotEqual(t, got.ExcessReads, got.RegressorRaw)

	_, err = leaves.LGEnsembleFromReader(bufio.NewReader(strings.NewReader(strings.Replace(
		strings.Replace(cls, "version=v4", "version=v3", 1), "binary sigmoid:1", "binary sigmoid:2", 1))), true)
	require.ErrorContains(t, err, "sigmoid with value != 1")
}

func TestPredictRejectsInput(t *testing.T) {
	m, err := Load("testdata", time.Hour)
	require.NoError(t, err)
	inf := make([]float64, len(featureNames))
	inf[6] = math.Inf(1)
	cases := []struct {
		name    string
		x       []float64
		wantErr string
	}{
		{name: "short vector", x: make([]float64, len(featureNames)-1), wantErr: "got 16 features"},
		{name: "infinite feature", x: inf, wantErr: "reads_1h is infinite"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := m.predictRaw(tt.x)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func readTestdata(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(data, []byte("tree\nversion=v4\n")))
	return string(data)
}

func loadPair(t *testing.T, c, r string) (*Model, error) {
	t.Helper()
	cls, err := loadEnsemble([]byte(readFile(t, c)), classifierObjective)
	if err != nil {
		return nil, err
	}
	reg, err := loadEnsemble([]byte(readFile(t, r)), regressorObjective)
	if err != nil {
		return nil, err
	}
	return &Model{classifier: cls, regressor: reg}, nil
}
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}
