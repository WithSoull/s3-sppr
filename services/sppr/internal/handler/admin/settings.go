package admin

import (
	"context"
	"math"
	"net/http"
	"strconv"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

func settingFields(s model.PlannerSettings) []field {
	return []field{
		{"horizon_seconds", "Горизонт, с", seconds(s.Horizon)},
		{"hot_fixed_ms", "HOT: фиксированное время чтения, мс", s.HotFixedMs}, {"hot_mib_per_ms", "HOT: скорость, МиБ/мс", s.HotMiBPerMs},
		{"cold_fixed_ms", "COLD: фиксированное время чтения, мс", s.ColdFixedMs}, {"cold_mib_per_ms", "COLD: скорость, МиБ/мс", s.ColdMiBPerMs},
		{"penalty_ms_per_mib", "Штраф, мс/МиБ", s.PenaltyMsPerMiB}, {"min_gain_ms", "Порог выигрыша Δmin, мс", s.MinGainMs},
		{"hot_utilization", "Допустимая загрузка HOT (0…1)", s.HotUtilization}, {"cold_utilization", "Допустимая загрузка COLD (0…1)", s.ColdUtilization},
	}
}
func parseSettings(r *http.Request) (model.PlannerSettings, error) {
	var s model.PlannerSettings
	var err error
	s.Horizon, err = duration(r.PostForm.Get("horizon_seconds"))
	if err != nil {
		return s, err
	}
	for _, f := range []struct {
		name string
		dst  *float64
	}{
		{"hot_fixed_ms", &s.HotFixedMs}, {"hot_mib_per_ms", &s.HotMiBPerMs}, {"cold_fixed_ms", &s.ColdFixedMs}, {"cold_mib_per_ms", &s.ColdMiBPerMs}, {"penalty_ms_per_mib", &s.PenaltyMsPerMiB}, {"min_gain_ms", &s.MinGainMs}, {"hot_utilization", &s.HotUtilization}, {"cold_utilization", &s.ColdUtilization},
	} {
		n, err := strconv.ParseFloat(r.PostForm.Get(f.name), 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			return s, model.ErrInvalidArgument
		}
		*f.dst = n
	}
	return s, nil
}
func (h *handler) update(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(w, r); err != nil {
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	req := model.CommandRequest{CommandID: r.PostForm.Get("command_id"), Initiator: access.Initiator(r.Context())}
	if !validID(req.CommandID) {
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	op := r.PathValue("operation")
	if op == "planner" {
		s, err := parseSettings(r)
		if err != nil {
			h.failure(w, r, err)
			return
		}
		req.Type = model.CommandSettings
		req.Settings = &s
		cmd, err := h.planner.Execute(r.Context(), req)
		if err != nil {
			h.failure(w, r, err)
			return
		}
		h.showCommand(w, r, cmd)
		return
	}
	req.Type = model.CommandType(op)
	var apply func(context.Context) error
	switch op {
	case "budget":
		n, err := strconv.ParseInt(r.PostForm.Get("bytes"), 10, 64)
		if err != nil {
			h.failure(w, r, model.ErrInvalidArgument)
			return
		}
		window, err := duration(r.PostForm.Get("window_seconds"))
		if err != nil {
			h.failure(w, r, err)
			return
		}
		apply = func(ctx context.Context) error { return h.migration.SetBudget(ctx, n, window) }
	case "interval":
		d, err := duration(r.PostForm.Get("min_interval_seconds"))
		if err != nil {
			h.failure(w, r, err)
			return
		}
		apply = func(ctx context.Context) error { return h.migration.SetMinInterval(ctx, d) }
	case "pin-add":
		p := model.Pin{Bucket: r.PostForm.Get("bucket"), Key: r.PostForm.Get("key"), Reason: r.PostForm.Get("reason"), Author: req.Initiator}
		apply = func(ctx context.Context) error { _, err := h.migration.AddPin(ctx, p); return err }
	case "pin-delete":
		apply = func(ctx context.Context) error {
			return h.migration.DeletePin(ctx, r.PostForm.Get("bucket"), r.PostForm.Get("key"))
		}
	default:
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	if h.forms == nil {
		h.fail(w, r, 503, "UPSTREAM_UNAVAILABLE")
		return
	}
	// Only posted fields enter the replay key, never query parameters.
	cmd, err := h.forms.Execute(r.Context(), req, op+"?"+r.PostForm.Encode(), apply)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.showCommand(w, r, cmd)
}
