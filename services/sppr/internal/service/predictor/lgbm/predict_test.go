package lgbm

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func TestFeatureVector(t *testing.T) {
	a, b := 4.0, 5.0
	f := model.Features{LogSize: 1, AgeSeconds: 2, Overwrites24h: 3, LastReadSeconds: &a, PreviousReadSeconds: &b, Reads5m: 6, Reads1h: 7, Reads6h: 8, Reads24h: 9, EWMA1h: 10, EWMA6h: 11, ShortLongRatio: 12, ReadsDayAgo: 13, MeanReadFraction: 14, BucketReads1h: 15, BucketActiveFraction: 16, ObservedSeconds: 17}
	want := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	require.Equal(t, want, featureVector(f, featureNames))
	require.Equal(t, []float64{17, 1, 10}, featureVector(f, []string{"observed_seconds", "log_size", "ewma_1h"}), "manifest order drives the vector")
	f.LastReadSeconds, f.PreviousReadSeconds = nil, nil
	got := featureVector(f, featureNames)
	require.True(t, math.IsNaN(got[3]))
	require.True(t, math.IsNaN(got[4]))
}

func TestPredict(t *testing.T) {
	m, err := Load("testdata", time.Hour)
	require.NoError(t, err)
	snapshot := model.FeatureSnapshot{VersionRef: model.VersionRef{Bucket: "b", ObjectKey: "k", VersionID: "v"}, At: time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC), RulesVersion: model.FeatureRulesVersion, Features: model.Features{EWMA1h: 12}, Quality: model.DataQuality{Usable: true, Reason: "complete"}}
	cases := []struct {
		name    string
		edit    func(*model.FeatureSnapshot)
		horizon time.Duration
		wantErr string
	}{
		{name: "forecast and hurdle explanation", horizon: time.Hour},
		{name: "wrong horizon", horizon: 2 * time.Hour, wantErr: "horizon"},
		{name: "rules", horizon: time.Hour, edit: func(s *model.FeatureSnapshot) { s.RulesVersion = "other" }, wantErr: "incompatible"},
		{name: "time missing", horizon: time.Hour, edit: func(s *model.FeatureSnapshot) { s.At = time.Time{} }, wantErr: "incompatible"},
		{name: "infinite feature", horizon: time.Hour, edit: func(s *model.FeatureSnapshot) { s.Features.LogSize = math.Inf(1) }, wantErr: "log_size is infinite"},
		{name: "invalid EWMA", horizon: time.Hour, edit: func(s *model.FeatureSnapshot) { s.Features.EWMA1h = math.NaN() }, wantErr: "EWMA comparison"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s := snapshot
			if tt.edit != nil {
				tt.edit(&s)
			}
			got, err := m.Predict(s, tt.horizon)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, s.VersionRef, got.VersionRef)
			require.Equal(t, s.At, got.At)
			require.Equal(t, s.Quality, got.Quality)
			require.Equal(t, MethodVersion, got.MethodVersion)
			require.Equal(t, "spike-fixture-v1", got.ModelVersion)
			require.Equal(t, int64(3600), got.HorizonSeconds)
			require.NotNil(t, got.Hurdle)
			require.Equal(t, 12.0, got.Hurdle.EWMAExpectedReads)
			require.Equal(t, got.ExpectedReads, got.Hurdle.Probability*got.Hurdle.ConditionalReads)
		})
	}
}

func TestPredictRejectsNonfiniteOutput(t *testing.T) {
	for _, tt := range []struct {
		name, score string
		classifier  bool
	}{
		{"NaN classifier", "nan", true}, {"infinite classifier", "inf", true}, {"NaN regressor", "nan", false}, {"exp overflow", "1000", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Load("testdata", time.Hour)
			require.NoError(t, err)
			name, objective := "regressor.txt", regressorObjective
			if tt.classifier {
				name, objective = "classifier.txt", classifierObjective
			}
			data := readTestdata(t, name)
			start := strings.Index(data, "leaf_value=")
			end := strings.IndexByte(data[start:], '\n') + start
			fields := strings.Fields(strings.TrimPrefix(data[start:end], "leaf_value="))
			for i := range fields {
				fields[i] = tt.score
			}
			data = data[:start] + "leaf_value=" + strings.Join(fields, " ") + data[end:]
			ensemble, err := loadEnsemble([]byte(data), objective)
			require.NoError(t, err)
			if tt.classifier {
				m.classifier = ensemble
			} else {
				m.regressor = ensemble
			}
			_, err = m.Predict(model.FeatureSnapshot{At: time.Now(), RulesVersion: model.FeatureRulesVersion}, time.Hour)
			require.ErrorContains(t, err, "non-finite forecast")
		})
	}
}
