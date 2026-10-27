package lgbm

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/dmitryikh/leaves"
)

const MethodVersion = "lgbm-hurdle-v1"

// featureNames is the features-v1 column order; it must match feature_names in both model files.
var featureNames = []string{
	"log_size",
	"age_seconds",
	"overwrites_24h",
	"last_read_seconds",
	"previous_read_seconds",
	"reads_5m",
	"reads_1h",
	"reads_6h",
	"reads_24h",
	"ewma_1h",
	"ewma_6h",
	"short_long_ratio",
	"reads_day_ago",
	"mean_read_fraction",
	"bucket_reads_1h",
	"bucket_active_fraction",
	"observed_seconds",
}

const (
	classifierObjective = "binary sigmoid:1"
	regressorObjective  = "poisson"
)

// Model is the two-stage (hurdle) predictor: E[N] = p * (1 + y), p = P(N >= 1), y ≈ N - 1 given N >= 1.
type Model struct {
	classifier *leaves.Ensemble
	regressor  *leaves.Ensemble
	manifest   Manifest
	horizon    time.Duration
}

type Prediction struct {
	ClassifierRaw    float64
	Probability      float64
	RegressorRaw     float64
	ExcessReads      float64
	ConditionalReads float64
	ExpectedReads    float64
}

var _ service.Predictor = (*Model)(nil)

func (m *Model) MethodVersion() string { return MethodVersion }
func (m *Model) ModelVersion() string  { return m.manifest.ModelVersion }

func (m *Model) Predict(s model.FeatureSnapshot, horizon time.Duration) (model.DemandForecast, error) {
	if s.RulesVersion != model.FeatureRulesVersion || s.At.IsZero() || horizon != m.horizon {
		return model.DemandForecast{}, fmt.Errorf("incompatible snapshot or horizon")
	}
	comparison, err := ewma.Predict(s, horizon)
	if err != nil {
		return model.DemandForecast{}, fmt.Errorf("EWMA comparison: %w", err)
	}
	p, err := m.predictRaw(featureVector(s.Features, m.manifest.FeatureNames))
	if err != nil {
		return model.DemandForecast{}, err
	}
	return model.DemandForecast{
		VersionRef: s.VersionRef, At: s.At, HorizonSeconds: int64(horizon / time.Second),
		ExpectedReads: p.ExpectedReads, Quality: s.Quality, MethodVersion: MethodVersion,
		ModelVersion: m.ModelVersion(), FeatureRulesVersion: s.RulesVersion,
		Hurdle: &model.HurdleForecast{Probability: p.Probability, ConditionalReads: p.ConditionalReads, EWMAExpectedReads: comparison.ExpectedReads},
	}, nil
}

func featureVector(f model.Features, names []string) []float64 {
	nullable := func(v *float64) float64 {
		if v == nil {
			return math.NaN()
		}
		return *v
	}
	values := map[string]float64{
		"log_size": f.LogSize, "age_seconds": f.AgeSeconds, "overwrites_24h": float64(f.Overwrites24h),
		"last_read_seconds": nullable(f.LastReadSeconds), "previous_read_seconds": nullable(f.PreviousReadSeconds),
		"reads_5m": float64(f.Reads5m), "reads_1h": float64(f.Reads1h), "reads_6h": float64(f.Reads6h), "reads_24h": float64(f.Reads24h),
		"ewma_1h": f.EWMA1h, "ewma_6h": f.EWMA6h, "short_long_ratio": f.ShortLongRatio, "reads_day_ago": float64(f.ReadsDayAgo),
		"mean_read_fraction": f.MeanReadFraction, "bucket_reads_1h": float64(f.BucketReads1h), "bucket_active_fraction": f.BucketActiveFraction, "observed_seconds": f.ObservedSeconds,
	}
	x := make([]float64, len(names))
	for i, name := range names {
		x[i] = values[name]
	}
	return x
}

// NaN is LightGBM's missing value, including the nullable recency features.
func (m *Model) predictRaw(x []float64) (p Prediction, err error) {
	// A malformed tree may only reach an invalid index on a particular input.
	defer func() {
		if reason := recover(); reason != nil {
			p, err = Prediction{}, fmt.Errorf("invalid LightGBM prediction: %v", reason)
		}
	}()
	if len(x) != len(featureNames) {
		return Prediction{}, fmt.Errorf("got %d features, want %d", len(x), len(featureNames))
	}
	for i, v := range x {
		if math.IsInf(v, 0) {
			return Prediction{}, fmt.Errorf("feature %s is infinite", featureNames[i])
		}
	}
	// Raw scores and explicit link functions: leaves applies them only for exact objective strings.
	p.ClassifierRaw = m.classifier.PredictSingle(x, 0)
	p.Probability = sigmoid(p.ClassifierRaw)
	p.RegressorRaw = m.regressor.PredictSingle(x, 0)
	p.ExcessReads = math.Exp(p.RegressorRaw)
	p.ConditionalReads = 1 + p.ExcessReads
	p.ExpectedReads = p.Probability * p.ConditionalReads
	for _, v := range []float64{p.ClassifierRaw, p.RegressorRaw, p.Probability, p.ExcessReads, p.ConditionalReads, p.ExpectedReads} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return Prediction{}, fmt.Errorf("non-finite forecast")
		}
	}
	if p.ExpectedReads < 0 {
		return Prediction{}, fmt.Errorf("negative forecast")
	}
	return p, nil
}

// sigmoid matches LightGBM's binary link with sigmoid:1.
func sigmoid(x float64) float64 {
	return 1 / (1 + math.Exp(-x))
}

func loadEnsemble(data []byte, objective string) (ensemble *leaves.Ensemble, err error) {
	// leaves indexes arrays from the text file without checking every length.
	// A malformed artifact must be a load error so startup can use EWMA.
	defer func() {
		if reason := recover(); reason != nil {
			ensemble, err = nil, fmt.Errorf("invalid LightGBM text: %v", reason)
		}
	}()
	data, err = checkHeader(data, objective)
	if err != nil {
		return nil, err
	}
	ensemble, err = leaves.LGEnsembleFromReader(bufio.NewReader(bytes.NewReader(data)), false)
	if err != nil {
		return nil, err
	}
	if ensemble.NFeatures() != len(featureNames) || ensemble.NRawOutputGroups() != 1 {
		return nil, fmt.Errorf("model dimensions: %d features, %d outputs; want %d features, 1 output", ensemble.NFeatures(), ensemble.NRawOutputGroups(), len(featureNames))
	}
	return ensemble, nil
}

// checkHeader validates the model contract and makes LightGBM 4 text readable by leaves.
// leaves rejects LightGBM 4 files (version=v4) although it reads their non-linear trees;
// linear trees are refused here and the parity test confirms the rest reads unchanged.
func checkHeader(data []byte, objective string) ([]byte, error) {
	header, _, _ := bytes.Cut(data, []byte("\n\n"))
	params := map[string]string{}
	for _, line := range strings.Split(string(header), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			params[k] = v
		} else if line == "average_output" {
			params[line] = ""
		}
	}
	if got := params["objective"]; got != objective {
		return nil, fmt.Errorf("objective %q, want %q", got, objective)
	}
	if got := strings.Fields(params["feature_names"]); !slices.Equal(got, featureNames) {
		return nil, fmt.Errorf("feature_names %v, want %v", got, featureNames)
	}
	if params["num_tree_per_iteration"] != "1" {
		return nil, fmt.Errorf("num_tree_per_iteration %q, want 1", params["num_tree_per_iteration"])
	}
	if _, ok := params["average_output"]; ok {
		return nil, fmt.Errorf("random forest models are not supported")
	}
	if bytes.Contains(data, []byte("\nis_linear=1\n")) {
		return nil, fmt.Errorf("linear trees are not supported by leaves")
	}
	switch params["version"] {
	case "v3":
		return data, nil
	case "v4":
		return bytes.Replace(data, []byte("version=v4\n"), []byte("version=v3\n"), 1), nil
	default:
		return nil, fmt.Errorf("unsupported model version %q", params["version"])
	}
}
