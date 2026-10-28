package planner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

const (
	Prefix       = "/api/planner"
	maxBodyBytes = 1 << 20
)

type handler struct{ service service.PlannerService }

// NewHandler serves the planner API under Prefix. Wrap it in
// access.Middleware: commands take their initiator from the request context.
func NewHandler(s service.PlannerService) http.Handler {
	h := &handler{service: s}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+Prefix+"/status", h.status)
	mux.HandleFunc("GET "+Prefix+"/plans", h.listPlans)
	mux.HandleFunc("GET "+Prefix+"/plans/latest", h.latestPlan)
	mux.HandleFunc("GET "+Prefix+"/plans/{id}", h.getPlan)
	mux.HandleFunc("GET "+Prefix+"/plans/{id}/recommendations", h.listRecommendations)
	mux.HandleFunc("POST "+Prefix+"/plans/recompute", h.command(model.CommandRecompute))
	mux.HandleFunc("POST "+Prefix+"/plans/{id}/confirm", h.command(model.CommandConfirm))
	mux.HandleFunc("POST "+Prefix+"/plans/{id}/reject", h.command(model.CommandReject))
	mux.HandleFunc("POST "+Prefix+"/commands/cancel-pending", h.command(model.CommandCancelPending))
	mux.HandleFunc("POST "+Prefix+"/commands/pause", h.command(model.CommandPause))
	mux.HandleFunc("POST "+Prefix+"/commands/resume", h.command(model.CommandResume))
	mux.HandleFunc("GET "+Prefix+"/commands", h.listCommands)
	mux.HandleFunc("GET "+Prefix+"/commands/{id}", h.getCommand)
	mux.HandleFunc("GET "+Prefix+"/settings", h.getSettings)
	mux.HandleFunc("PUT "+Prefix+"/settings", h.putSettings)
	return mux
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	st, err := h.service.Status(r.Context())
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	dto := statusDTO{
		History: st.History, HistoryLagSeconds: st.HistoryLagSeconds, Objects: st.Objects,
		Hot:            classDTO{st.Hot.CapacityBytes, st.Hot.MaxUtilization, st.Hot.UsedBytes},
		Cold:           classDTO{st.Cold.CapacityBytes, st.Cold.MaxUtilization, st.Cold.UsedBytes},
		HistoryQuality: st.HistoryQuality, MethodVersion: st.MethodVersion, ModelVersion: st.ModelVersion, FallbackReason: st.FallbackReason,
		Paused: st.Migration.Paused, MinIntervalSec: int64(st.Migration.MinInterval / time.Second),
		BudgetBytes: st.Budget.Bytes, BudgetWindowS: int64(st.Budget.Window / time.Second),
		BudgetSpent: st.Budget.Spent, BudgetLeft: st.Budget.Remaining,
		PendingActions: st.PendingActions, RunningActions: st.RunningActions,
	}
	if st.LatestPlan != nil {
		p := planFromModel(*st.LatestPlan)
		dto.LatestPlan = &p
	}
	writeJSON(w, http.StatusOK, dto)
}

func (h *handler) listPlans(w http.ResponseWriter, r *http.Request) {
	size, err := pageSize(r)
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	page, err := h.service.ListPlans(r.Context(), model.PlanFilter{PageSize: size, PageToken: r.URL.Query().Get("page_token")})
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	plans := make([]planDTO, 0, len(page.Plans))
	for _, p := range page.Plans {
		plans = append(plans, planFromModel(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"plans": plans, "next_page_token": page.NextPageToken})
}

func (h *handler) latestPlan(w http.ResponseWriter, r *http.Request) {
	p, err := h.service.LatestPlan(r.Context())
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	if p == nil {
		writeError(r.Context(), w, model.ErrPlanNotFound)
		return
	}
	writeJSON(w, http.StatusOK, planFromModel(*p))
}

func (h *handler) getPlan(w http.ResponseWriter, r *http.Request) {
	p, err := h.service.GetPlan(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, planFromModel(p))
}

func (h *handler) listRecommendations(w http.ResponseWriter, r *http.Request) {
	size, err := pageSize(r)
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	q := r.URL.Query()
	f := model.RecommendationFilter{PlanID: r.PathValue("id"), Kind: model.RecommendationKind(q.Get("kind")), PageSize: size, PageToken: q.Get("page_token")}
	if raw := q.Get("selected"); raw != "" {
		selected, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(r.Context(), w, fmt.Errorf("%w: selected must be true or false", model.ErrInvalidArgument))
			return
		}
		f.Selected = &selected
	}
	page, err := h.service.ListRecommendations(r.Context(), f)
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	recs := make([]recommendationDTO, 0, len(page.Recommendations))
	for _, rec := range page.Recommendations {
		recs = append(recs, recommendationFromModel(rec))
	}
	writeJSON(w, http.StatusOK, map[string]any{"recommendations": recs, "next_page_token": page.NextPageToken})
}

// command answers 202 when the command is accepted (the work it starts runs
// later) and 409 with the administrator code when the rules refuse it.
func (h *handler) command(t model.CommandType) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body commandRequest
		if err := decode(r, &body); err != nil {
			writeError(r.Context(), w, err)
			return
		}
		req := model.CommandRequest{CommandID: body.CommandID, Initiator: access.Initiator(r.Context()), Type: t, PlanID: r.PathValue("id")}
		switch t {
		case model.CommandConfirm:
			req.RecommendationIDs = body.RecommendationIDs
		case model.CommandCancelPending:
			req.PlanID, req.RecommendationIDs = body.PlanID, body.ActionIDs
		}
		h.execute(w, r, req)
	}
}

func (h *handler) execute(w http.ResponseWriter, r *http.Request, req model.CommandRequest) {
	cmd, err := h.service.Execute(r.Context(), req)
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	status := http.StatusAccepted
	if cmd.Status == model.CommandRejected {
		status = http.StatusConflict
	}
	writeJSON(w, status, commandFromModel(cmd))
}

func (h *handler) listCommands(w http.ResponseWriter, r *http.Request) {
	size, err := pageSize(r)
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	q := r.URL.Query()
	page, err := h.service.ListCommands(r.Context(), model.CommandFilter{PlanID: q.Get("plan_id"), PageSize: size, PageToken: q.Get("page_token")})
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	commands := make([]commandDTO, 0, len(page.Commands))
	for _, c := range page.Commands {
		commands = append(commands, commandFromModel(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"commands": commands, "next_page_token": page.NextPageToken})
}

func (h *handler) getCommand(w http.ResponseWriter, r *http.Request) {
	cmd, err := h.service.GetCommand(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, commandFromModel(cmd))
}

func (h *handler) getSettings(w http.ResponseWriter, r *http.Request) {
	s, err := h.service.GetSettings(r.Context())
	if err != nil {
		writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, paramsFromSettings(s))
}

func (h *handler) putSettings(w http.ResponseWriter, r *http.Request) {
	var body settingsRequest
	if err := decode(r, &body); err != nil {
		writeError(r.Context(), w, err)
		return
	}
	settings := body.toModel()
	h.execute(w, r, model.CommandRequest{CommandID: body.CommandID, Initiator: access.Initiator(r.Context()), Type: model.CommandSettings, Settings: &settings})
}

func pageSize(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("page_size")
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%w: page_size must be a positive integer", model.ErrInvalidArgument)
	}
	return n, nil
}

func decode(r *http.Request, dst any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("%w: request body: %v", model.ErrInvalidArgument, err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(ctx context.Context, w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, model.ErrInvalidArgument):
		status = http.StatusBadRequest
	case errors.Is(err, model.ErrPlanNotFound), errors.Is(err, model.ErrCommandNotFound):
		status = http.StatusNotFound
	case errors.Is(err, model.ErrCommandConflict), errors.Is(err, model.ErrActionConflict):
		status = http.StatusConflict
	case errors.Is(err, model.ErrAccessDenied):
		status = http.StatusForbidden
	}
	message := err.Error()
	if status == http.StatusInternalServerError {
		logger.Error(ctx, "planner API request failed", zap.Error(err))
		message = "internal error"
	}
	writeJSON(w, status, errorResponse{Error: message})
}
