package lgbm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func copyFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"manifest.json", "classifier.txt", "regressor.txt", "parity.json"} {
		data, err := os.ReadFile(filepath.Join("testdata", name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
	}
	return dir
}

func changeManifest(t *testing.T, dir string, edit func(*Manifest)) {
	t.Helper()
	var m Manifest
	require.NoError(t, json.Unmarshal([]byte(readFile(t, filepath.Join(dir, "manifest.json"))), &m))
	edit(&m)
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0600))
}

func TestLoadDirectory(t *testing.T) {
	cases := []struct {
		name    string
		edit    func(*testing.T, string)
		horizon time.Duration
		wantErr string
	}{
		{name: "fixture", horizon: time.Hour},
		{name: "rules", horizon: time.Hour, edit: func(t *testing.T, d string) {
			changeManifest(t, d, func(m *Manifest) { m.FeatureRulesVersion = "features-v2" })
		}, wantErr: "feature_rules_version"},
		{name: "method", horizon: time.Hour, edit: func(t *testing.T, d string) { changeManifest(t, d, func(m *Manifest) { m.MethodVersion = "lgbm-v2" }) }, wantErr: "method_version"},
		{name: "model version missing", horizon: time.Hour, edit: func(t *testing.T, d string) { changeManifest(t, d, func(m *Manifest) { m.ModelVersion = "" }) }, wantErr: "model_version"},
		{name: "unknown feature", horizon: time.Hour, edit: func(t *testing.T, d string) {
			changeManifest(t, d, func(m *Manifest) { m.FeatureNames[0] = "unknown" })
		}, wantErr: "feature_names"},
		{name: "feature order", horizon: time.Hour, edit: func(t *testing.T, d string) {
			changeManifest(t, d, func(m *Manifest) { m.FeatureNames[0], m.FeatureNames[1] = m.FeatureNames[1], m.FeatureNames[0] })
		}, wantErr: "feature_names"},
		{name: "feature absent", horizon: time.Hour, edit: func(t *testing.T, d string) {
			changeManifest(t, d, func(m *Manifest) { m.FeatureNames = m.FeatureNames[1:] })
		}, wantErr: "feature_names"},
		{name: "horizon mismatch", horizon: 2 * time.Hour, wantErr: "horizon_seconds"},
		{name: "fractional horizon", horizon: time.Hour + time.Millisecond, wantErr: "horizon_seconds"},
		{name: "zero horizon", wantErr: "horizon_seconds"},
		{name: "classifier hash", horizon: time.Hour, edit: func(t *testing.T, d string) {
			changeManifest(t, d, func(m *Manifest) { m.ClassifierSHA256 = strings.Repeat("0", 64) })
		}, wantErr: "classifier.txt: SHA-256 mismatch"},
		{name: "regressor hash", horizon: time.Hour, edit: func(t *testing.T, d string) {
			changeManifest(t, d, func(m *Manifest) { m.RegressorSHA256 = strings.Repeat("0", 64) })
		}, wantErr: "regressor.txt: SHA-256 mismatch"},
		{name: "malformed digest", horizon: time.Hour, edit: func(t *testing.T, d string) { changeManifest(t, d, func(m *Manifest) { m.DataSHA256 = "invalid" }) }, wantErr: "data_sha256"},
		{name: "empty parity", horizon: time.Hour, edit: func(t *testing.T, d string) {
			require.NoError(t, os.WriteFile(filepath.Join(d, "parity.json"), []byte(`{"rows":[]}`), 0600))
		}, wantErr: "no control rows"},
		{name: "wrong parity", horizon: time.Hour, edit: func(t *testing.T, d string) {
			mutateParity(t, d, func(f *parityFixture) { v := 0.99; f.Rows[0].Want["probability"] = &v })
		}, wantErr: "parity mismatch"},
		{name: "raw output absent", horizon: time.Hour, edit: func(t *testing.T, d string) {
			mutateParity(t, d, func(f *parityFixture) { delete(f.Rows[0].Want, "classifier_raw") })
		}, wantErr: "classifier_raw parity mismatch"},
		{name: "parity feature absent", horizon: time.Hour, edit: func(t *testing.T, d string) {
			mutateParity(t, d, func(f *parityFixture) { delete(f.Rows[0].Features, "log_size") })
		}, wantErr: "feature_names mismatch"},
	}
	for _, name := range []string{"manifest.json", "classifier.txt", "regressor.txt", "parity.json"} {
		cases = append(cases, struct {
			name    string
			edit    func(*testing.T, string)
			horizon time.Duration
			wantErr string
		}{
			name: "missing " + name, horizon: time.Hour, edit: func(t *testing.T, d string) { require.NoError(t, os.Remove(filepath.Join(d, name))) }, wantErr: name})
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			dir := copyFixture(t)
			if tt.edit != nil {
				tt.edit(t, dir)
			}
			got, err := Load(dir, tt.horizon)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, MethodVersion, got.MethodVersion())
			require.Equal(t, "spike-fixture-v1", got.ModelVersion())
		})
	}
}

func mutateParity(t *testing.T, dir string, edit func(*parityFixture)) {
	t.Helper()
	var f parityFixture
	require.NoError(t, json.Unmarshal([]byte(readFile(t, filepath.Join(dir, "parity.json"))), &f))
	edit(&f)
	data, err := json.Marshal(f)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "parity.json"), data, 0600))
}

func TestParityTolerance(t *testing.T) {
	require.Equal(t, 1e-9, parityRelativeTolerance)
	for _, tt := range []struct {
		name    string
		delta   float64
		wantErr bool
	}{
		{"within relative tolerance", 0.5e-9, false}, {"outside relative tolerance", 2e-9, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := copyFixture(t)
			mutateParity(t, dir, func(f *parityFixture) { *f.Rows[0].Want["expected_reads"] *= 1 + tt.delta })
			_, err := Load(dir, time.Hour)
			if tt.wantErr {
				require.ErrorContains(t, err, "expected_reads parity mismatch")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLoadMalformedLeafValues(t *testing.T) {
	dir := copyFixture(t)
	path := filepath.Join(dir, "classifier.txt")
	data := []byte(regexp.MustCompile(`(?m)^leaf_value=.*$`).ReplaceAllString(readFile(t, path), "leaf_value=0"))
	require.NoError(t, os.WriteFile(path, data, 0600))
	sum := sha256.Sum256(data)
	changeManifest(t, dir, func(m *Manifest) { m.ClassifierSHA256 = hex.EncodeToString(sum[:]) })
	var err error
	require.NotPanics(t, func() { _, err = Load(dir, time.Hour) })
	require.ErrorContains(t, err, "invalid LightGBM prediction")
}
