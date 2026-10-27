package lgbm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

// Manifest is the shared export contract with the offline trainer. Training
// metadata is retained for provenance, never used to tune a model at startup.
type Manifest struct {
	ModelVersion        string          `json:"model_version"`
	MethodVersion       string          `json:"method_version"`
	FeatureRulesVersion string          `json:"feature_rules_version"`
	FeatureNames        []string        `json:"feature_names"`
	HorizonSeconds      int64           `json:"horizon_seconds"`
	LightGBMVersion     string          `json:"lightgbm_version"`
	PythonVersion       string          `json:"python_version"`
	NumpyVersion        string          `json:"numpy_version"`
	Hyperparameters     json.RawMessage `json:"hyperparameters"`
	TrainInterval       json.RawMessage `json:"train_interval"`
	ValidationInterval  json.RawMessage `json:"validation_interval"`
	Source              string          `json:"source"`
	DataSHA256          string          `json:"data_sha256"`
	ValidationMetrics   json.RawMessage `json:"validation_metrics"`
	ClassifierSHA256    string          `json:"classifier_sha256"`
	RegressorSHA256     string          `json:"regressor_sha256"`
}

// Load validates the entire artifact before making it available to the planner.
func Load(dir string, horizon time.Duration) (*Model, error) {
	if dir == "" {
		return nil, fmt.Errorf("PLANNER_MODEL_DIR is empty")
	}
	var manifest Manifest
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("manifest.json: %w", err)
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("manifest.json: %w", err)
	}
	if err := manifest.validate(horizon); err != nil {
		return nil, fmt.Errorf("manifest.json: %w", err)
	}
	cls, err := readVerified(dir, "classifier.txt", manifest.ClassifierSHA256)
	if err != nil {
		return nil, err
	}
	reg, err := readVerified(dir, "regressor.txt", manifest.RegressorSHA256)
	if err != nil {
		return nil, err
	}
	classifier, err := loadEnsemble(cls, classifierObjective)
	if err != nil {
		return nil, fmt.Errorf("classifier.txt: %w", err)
	}
	regressor, err := loadEnsemble(reg, regressorObjective)
	if err != nil {
		return nil, fmt.Errorf("regressor.txt: %w", err)
	}
	m := &Model{classifier: classifier, regressor: regressor, manifest: manifest, horizon: horizon}
	data, err = os.ReadFile(filepath.Join(dir, "parity.json"))
	if err != nil {
		return nil, fmt.Errorf("parity.json: %w", err)
	}
	if err := m.checkParity(data); err != nil {
		return nil, fmt.Errorf("parity.json: %w", err)
	}
	return m, nil
}

func (m Manifest) validate(horizon time.Duration) error {
	switch {
	case strings.TrimSpace(m.ModelVersion) == "":
		return fmt.Errorf("model_version is empty")
	case m.MethodVersion != MethodVersion:
		return fmt.Errorf("method_version %q, want %q", m.MethodVersion, MethodVersion)
	case m.FeatureRulesVersion != model.FeatureRulesVersion:
		return fmt.Errorf("feature_rules_version %q, want %q", m.FeatureRulesVersion, model.FeatureRulesVersion)
	case !slices.Equal(m.FeatureNames, featureNames):
		return fmt.Errorf("feature_names %v, want %v", m.FeatureNames, featureNames)
	case horizon <= 0 || horizon%time.Second != 0 || m.HorizonSeconds != int64(horizon/time.Second):
		return fmt.Errorf("horizon_seconds %d does not match planner horizon %s", m.HorizonSeconds, horizon)
	case m.LightGBMVersion == "" || m.PythonVersion == "" || m.NumpyVersion == "" || m.Source == "":
		return fmt.Errorf("missing training provenance (versions/source)")
	}
	for name, value := range map[string]json.RawMessage{
		"hyperparameters": m.Hyperparameters, "train_interval": m.TrainInterval,
		"validation_interval": m.ValidationInterval, "validation_metrics": m.ValidationMetrics,
	} {
		var object map[string]json.RawMessage
		if len(value) == 0 || json.Unmarshal(value, &object) != nil || object == nil {
			return fmt.Errorf("%s must be an object", name)
		}
	}
	for name, value := range map[string]string{"data_sha256": m.DataSHA256, "classifier_sha256": m.ClassifierSHA256, "regressor_sha256": m.RegressorSHA256} {
		raw, err := hex.DecodeString(value)
		if err != nil || len(raw) != sha256.Size {
			return fmt.Errorf("%s must be a SHA-256 hex digest", name)
		}
	}
	return nil
}

func readVerified(dir, name, want string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	sum := sha256.Sum256(data)
	if !strings.EqualFold(hex.EncodeToString(sum[:]), want) {
		return nil, fmt.Errorf("%s: SHA-256 mismatch", name)
	}
	return data, nil
}

const parityRelativeTolerance = 1e-9

type parityFixture struct {
	Rows []struct {
		Name     string              `json:"name"`
		Features map[string]*float64 `json:"features"`
		Want     map[string]*float64 `json:"want"`
	} `json:"rows"`
}

func (m *Model) checkParity(data []byte) error {
	var fixture parityFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		return err
	}
	if len(fixture.Rows) == 0 {
		return fmt.Errorf("no control rows")
	}
	for i, row := range fixture.Rows {
		if len(row.Features) != len(m.manifest.FeatureNames) {
			return fmt.Errorf("row %d %q: feature_names mismatch", i, row.Name)
		}
		x := make([]float64, len(m.manifest.FeatureNames))
		for j, name := range m.manifest.FeatureNames {
			v, ok := row.Features[name]
			if !ok {
				return fmt.Errorf("row %d %q: missing feature %s", i, row.Name, name)
			}
			x[j] = math.NaN()
			if v != nil {
				x[j] = *v
			}
		}
		p, err := m.predictRaw(x)
		if err != nil {
			return fmt.Errorf("row %d %q: %w", i, row.Name, err)
		}
		for name, got := range map[string]float64{
			"classifier_raw": p.ClassifierRaw, "regressor_raw": p.RegressorRaw,
			"probability": p.Probability, "conditional_reads": p.ConditionalReads, "expected_reads": p.ExpectedReads,
		} {
			want := row.Want[name]
			// A zero reference requires an exact zero; all other values use relative error.
			if want == nil {
				return fmt.Errorf("row %d %q: %s parity mismatch: missing reference", i, row.Name, name)
			}
			if math.IsNaN(*want) || math.IsInf(*want, 0) || math.Abs(got-*want) > parityRelativeTolerance*math.Abs(*want) {
				return fmt.Errorf("row %d %q: %s parity mismatch (got %.17g, reference %.17g)", i, row.Name, name, got, *want)
			}
		}
	}
	return nil
}
