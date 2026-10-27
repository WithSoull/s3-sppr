package ewma_test

import (
	"math"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/predictor/ewma"
	"github.com/stretchr/testify/require"
)

func TestPredict(t *testing.T) {
	cases := []struct {
		name    string
		rate    float64
		horizon time.Duration
		quality model.DataQuality
		want    float64
		wantErr bool
	}{
		{"hour", 12, time.Hour, model.DataQuality{Usable: true, Reason: "complete"}, 12, false},
		{"half hour", 12, 30 * time.Minute, model.DataQuality{Usable: true}, 6, false},
		{"zero", 0, time.Hour, model.DataQuality{Usable: true}, 0, false},
		{"unusable data is preserved", 12, time.Hour, model.DataQuality{Reason: "gap"}, 12, false},
		{"nan", math.NaN(), time.Hour, model.DataQuality{}, 0, true},
		{"infinity", math.Inf(1), time.Hour, model.DataQuality{}, 0, true},
		{"negative", -1, time.Hour, model.DataQuality{}, 0, true},
		{"zero horizon", 1, 0, model.DataQuality{}, 0, true},
		{"subsecond horizon", 1, time.Millisecond, model.DataQuality{}, 0, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s := model.FeatureSnapshot{VersionRef: model.VersionRef{Bucket: "b", ObjectKey: "k", VersionID: "v"}, At: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), RulesVersion: model.FeatureRulesVersion, Features: model.Features{EWMA1h: tt.rate}, Quality: tt.quality}
			got, err := ewma.Predict(s, tt.horizon)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got.ExpectedReads)
			require.Equal(t, tt.quality, got.Quality)
			require.Equal(t, s.VersionRef, got.VersionRef)
			require.Equal(t, ewma.MethodVersion, got.MethodVersion)
		})
	}
	_, err := ewma.Predict(model.FeatureSnapshot{RulesVersion: "future-v2"}, time.Hour)
	require.Error(t, err)
}
