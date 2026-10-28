package planner

import (
	"context"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestPlanCapturesOnePredictorAcrossSwap(t *testing.T) {
	mc := minimock.NewController(t)
	d := newCycleDeps(mc)
	d.world(model.Coverage{Known: true, Start: cycleNow.Add(-72 * time.Hour), Watermark: cycleNow})
	lifecycle := mocks.NewModelLifecycleMock(mc)
	a, b := mocks.NewPredictorMock(mc), mocks.NewPredictorMock(mc)
	for _, p := range []*mocks.PredictorMock{a, b} {
		p.MethodVersionMock.Optional().Return("test-ml")
	}
	a.ModelVersionMock.Return("A")
	current := service.Predictor(a)
	lifecycle.CaptureMock.Set(func(context.Context, time.Duration) service.Predictor { return current })
	a.PredictMock.Set(func(row model.FeatureSnapshot, h time.Duration) (model.DemandForecast, error) {
		current = b
		return model.DemandForecast{At: row.At, ExpectedReads: 3, ModelVersion: "A", MethodVersion: "test-ml", HorizonSeconds: int64(h / time.Second)}, nil
	})
	s := d.service()
	s.cfg.Models = lifecycle
	plan, e := s.Plan(context.Background(), model.TriggerManual)
	require.NoError(t, e)
	require.Equal(t, "A", plan.ModelVersion)
	require.Equal(t, service.Predictor(b), current)
	require.Greater(t, len(d.recs), 1)
	for _, r := range d.recs {
		if r.Explanation.Forecast.ExpectedReads > 0 {
			require.Equal(t, "test-ml", r.Explanation.Forecast.Method)
		}
	}
	// No calls are allowed on B, even after the swap during A's first prediction.
	require.EqualValues(t, 1, lifecycle.CaptureAfterCounter())
}
func TestRetrainReplayAndRefusal(t *testing.T) {
	for _, tt := range []struct {
		name, reason string
		status       model.CommandStatus
	}{{"accepted", "", model.CommandAccepted}, {"busy", "training_in_progress", model.CommandRejected}, {"incomplete", "insufficient_history", model.CommandRejected}} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			d := newCmdDeps(mc)
			s := d.service()
			lifecycle := mocks.NewModelLifecycleMock(mc)
			s.cfg.Models = lifecycle
			d.plans.GetCommandMock.Set(func(context.Context, string) (model.Command, error) {
				if len(d.stored) > 0 {
					return d.stored[0], nil
				}
				return model.Command{}, model.ErrCommandNotFound
			})
			lifecycle.ReserveMock.Return(tt.reason, nil)
			if tt.reason == "" {
				lifecycle.QueueMock.Expect(minimock.AnyContext, "manual").Return(nil)
				lifecycle.StartMock.Expect("manual")
			}
			req := model.CommandRequest{CommandID: cmdID, Type: model.CommandRetrain, Initiator: userID}
			first, e := s.Execute(context.Background(), req)
			require.NoError(t, e)
			require.Equal(t, tt.status, first.Status)
			again, e := s.Execute(context.Background(), req)
			require.NoError(t, e)
			require.Equal(t, first, again)
			require.Len(t, d.stored, 1)
			require.EqualValues(t, 1, lifecycle.ReserveAfterCounter())
		})
	}
}
