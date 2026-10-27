package ewma

import (
	"fmt"
	"math"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

const MethodVersion = "ewma-1h-v1"

func Predict(s model.FeatureSnapshot, horizon time.Duration) (model.DemandForecast, error) {
	if s.RulesVersion != model.FeatureRulesVersion || horizon <= 0 || horizon%time.Second != 0 || s.At.IsZero() || math.IsNaN(s.Features.EWMA1h) || math.IsInf(s.Features.EWMA1h, 0) || s.Features.EWMA1h < 0 {
		return model.DemandForecast{}, fmt.Errorf("incompatible snapshot or invalid EWMA/horizon")
	}
	n := s.Features.EWMA1h * horizon.Hours()
	if math.IsInf(n, 0) {
		return model.DemandForecast{}, fmt.Errorf("EWMA overflow")
	}
	return model.DemandForecast{VersionRef: s.VersionRef, At: s.At, HorizonSeconds: int64(horizon / time.Second), ExpectedReads: n, Quality: s.Quality, MethodVersion: MethodVersion, FeatureRulesVersion: s.RulesVersion}, nil
}
