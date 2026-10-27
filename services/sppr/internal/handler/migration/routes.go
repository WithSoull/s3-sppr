package migration

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

func (h *handler) submit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := decode(r, &req); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	specs := make([]model.ActionSpec, 0, len(req.Actions))
	for _, a := range req.Actions {
		specs = append(specs, model.ActionSpec{
			ActionID:           a.ActionID,
			VersionID:          a.VersionID,
			ExpectedGeneration: a.ExpectedGeneration,
			SourceClass:        model.StorageClass(a.SourceClass),
			TargetClass:        model.StorageClass(a.TargetClass),
			SizeBytes:          a.SizeBytes,
			DependsOn:          a.DependsOn,
		})
	}
	result, err := h.service.SubmitActions(r.Context(), req.PlanID, specs)
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, submitResponse{Accepted: result.Accepted, Existing: result.Existing})
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	filter := model.ActionFilter{
		PlanID:    query.Get("plan_id"),
		Status:    model.ActionStatus(query.Get("status")),
		PageToken: query.Get("page_token"),
	}
	if raw := query.Get("page_size"); raw != "" {
		size, err := strconv.Atoi(raw)
		if err != nil || size < 1 {
			writeError(r.Context(), w, invalid("page_size must be a positive integer"))
			return
		}
		filter.PageSize = size
	}
	page, err := h.service.ListActions(r.Context(), filter)
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	response := listResponse{Actions: make([]actionDTO, 0, len(page.Actions)), NextPageToken: page.NextPageToken}
	for _, view := range page.Actions {
		response.Actions = append(response.Actions, actionFromModel(view.Action, view.BlockedBy))
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	details, err := h.service.GetAction(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	response := actionDetailsDTO{actionDTO: actionFromModel(details.Action, details.BlockedBy), Events: make([]eventDTO, 0, len(details.Events))}
	for _, e := range details.Events {
		response.Events = append(response.Events, eventDTO{
			At: e.At, FromStatus: string(e.FromStatus), ToStatus: string(e.ToStatus),
			FromStep: string(e.FromStep), ToStep: string(e.ToStep),
			Bytes: e.Bytes, Reason: string(e.Reason), Error: e.Error,
		})
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *handler) cancel(w http.ResponseWriter, r *http.Request) {
	var req cancelRequest
	if err := decode(r, &req); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	result, err := h.service.CancelActions(r.Context(), model.CancelRequest{PlanID: req.PlanID, ActionIDs: req.ActionIDs})
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, cancelResponse{Cancelled: result.Cancelled, Dependents: result.Dependents})
}

func (h *handler) pause(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h.service.SetPaused(r.Context(), paused); err != nil {
			writeError(r.Context(), w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"paused": paused})
	}
}

func (h *handler) getBudget(w http.ResponseWriter, r *http.Request) {
	budget, err := h.service.GetBudget(r.Context())
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, budgetDTO{
		Bytes: budget.Bytes, WindowSeconds: int64(budget.Window / time.Second),
		Spent: &budget.Spent, Remaining: &budget.Remaining,
	})
}

func (h *handler) putBudget(w http.ResponseWriter, r *http.Request) {
	var req budgetDTO
	if err := decode(r, &req); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	if req.Spent != nil || req.Remaining != nil {
		writeError(r.Context(), w, invalid("spent and remaining are read-only"))
		return
	}
	if req.WindowSeconds <= 0 {
		writeError(r.Context(), w, invalid("window_seconds must be positive"))
		return
	}
	if err := h.service.SetBudget(r.Context(), req.Bytes, time.Duration(req.WindowSeconds)*time.Second); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	h.getBudget(w, r)
}

func (h *handler) getSettings(w http.ResponseWriter, r *http.Request) {
	settings, err := h.service.GetSettings(r.Context())
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, settingsDTO{MinIntervalSeconds: int64(settings.MinInterval / time.Second), Paused: &settings.Paused})
}

func (h *handler) putSettings(w http.ResponseWriter, r *http.Request) {
	var req settingsDTO
	if err := decode(r, &req); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	if req.Paused != nil {
		writeError(r.Context(), w, invalid("paused is changed by POST /pause and /resume"))
		return
	}
	if req.MinIntervalSeconds < 0 {
		writeError(r.Context(), w, invalid("min_interval_seconds must not be negative"))
		return
	}
	if err := h.service.SetMinInterval(r.Context(), time.Duration(req.MinIntervalSeconds)*time.Second); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	h.getSettings(w, r)
}

func (h *handler) listPins(w http.ResponseWriter, r *http.Request) {
	pins, err := h.service.ListPins(r.Context())
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	response := make([]pinDTO, 0, len(pins))
	for _, p := range pins {
		response = append(response, pinFromModel(p))
	}
	writeJSON(w, http.StatusOK, map[string][]pinDTO{"pins": response})
}

func (h *handler) addPin(w http.ResponseWriter, r *http.Request) {
	var req pinDTO
	if err := decode(r, &req); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	if req.CreatedAt != nil {
		writeError(r.Context(), w, invalid("created_at is read-only"))
		return
	}
	pin, err := h.service.AddPin(r.Context(), model.Pin{Bucket: req.Bucket, Key: req.Key, Reason: req.Reason, Author: req.Author})
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusCreated, pinFromModel(pin))
}

// Keys may contain slashes, so the pin is named by query parameters.
func (h *handler) deletePin(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if err := h.service.DeletePin(r.Context(), query.Get("bucket"), query.Get("key")); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", model.ErrInvalidArgument, message)
}
