package lgbm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type spikeFixture struct {
	Meta struct {
		Features          []string `json:"features"`
		LightGBM          string   `json:"lightgbm"`
		ClassifierSHA256  string   `json:"classifier_sha256"`
		RegressorSHA256   string   `json:"regressor_sha256"`
		TrainRows         int      `json:"train_rows"`
		TrainPositiveRows int      `json:"train_positive_rows"`
	} `json:"meta"`
	Rows []struct {
		Name     string              `json:"name"`
		Features map[string]*float64 `json:"features"`
		Want     struct {
			ClassifierRaw    float64 `json:"classifier_raw"`
			RegressorRaw     float64 `json:"regressor_raw"`
			Probability      float64 `json:"probability"`
			ExcessReads      float64 `json:"excess_reads"`
			ConditionalReads float64 `json:"conditional_reads"`
			ExpectedReads    float64 `json:"expected_reads"`
		} `json:"want"`
	} `json:"rows"`
}

func TestParityWithPython(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "parity.json"))
	require.NoError(t, err)
	var fx spikeFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	require.Equal(t, 1e-9, parityRelativeTolerance)
	require.Equal(t, featureNames, fx.Meta.Features)
	require.Equal(t, fx.Meta.ClassifierSHA256, fileSHA256(t, "classifier.txt"), "spike fixture SHA-256 changed")
	require.Equal(t, fx.Meta.RegressorSHA256, fileSHA256(t, "regressor.txt"), "spike fixture SHA-256 changed")
	require.NotEmpty(t, fx.Rows)

	m, err := Load("testdata", time.Hour)
	require.NoError(t, err)

	var maxProb, maxReadsRel float64
	for _, row := range fx.Rows {
		x := make([]float64, len(featureNames))
		for i, name := range featureNames {
			v, ok := row.Features[name]
			require.True(t, ok, "%s: feature %s absent", row.Name, name)
			x[i] = math.NaN()
			if v != nil {
				x[i] = *v
			}
		}
		got, err := m.predictRaw(x)
		require.NoError(t, err, row.Name)

		dp := math.Abs(got.Probability - row.Want.Probability)
		require.LessOrEqual(t, dp, 1e-9*math.Abs(row.Want.Probability), "%s probability", row.Name)
		maxProb = math.Max(maxProb, dp)
		for _, c := range []struct {
			what      string
			got, want float64
		}{
			{"classifier_raw", got.ClassifierRaw, row.Want.ClassifierRaw},
			{"regressor_raw", got.RegressorRaw, row.Want.RegressorRaw},
			{"excess", got.ExcessReads, row.Want.ExcessReads},
			{"conditional", got.ConditionalReads, row.Want.ConditionalReads},
			{"expected", got.ExpectedReads, row.Want.ExpectedReads},
		} {
			d := math.Abs(c.got - c.want)
			require.LessOrEqual(t, d, 1e-9*math.Abs(c.want), "%s %s: got %v want %v", row.Name, c.what, c.got, c.want)
			if c.want != 0 {
				maxReadsRel = math.Max(maxReadsRel, d/math.Abs(c.want))
			}
		}
	}
	t.Logf("lightgbm=%s rows=%d train=%d positives=%d max|dp|=%.3g max relative output error=%.3g",
		fx.Meta.LightGBM, len(fx.Rows), fx.Meta.TrainRows, fx.Meta.TrainPositiveRows, maxProb, maxReadsRel)
}

func fileSHA256(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
