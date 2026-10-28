package admin

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client/identity"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/google/uuid"
)

//go:embed templates/*.html
var templates embed.FS

type handler struct {
	planner    service.PlannerService
	migration  service.MigrationService
	forms      service.FormCommands
	auth       identity.AuthClient
	authorizer access.Authorizer
	templates  *template.Template
}
type field struct {
	Name, Label string
	Value       any
}
type view struct {
	Models                                 []model.ModelVersion
	Page                                   string
	Authenticated                          bool
	Message, Code                          string
	Status                                 model.PlannerStatus
	Settings                               model.PlannerSettings
	Migration                              model.MigrationSettings
	Budget                                 model.BudgetStatus
	Fields                                 []field
	Pins                                   []model.Pin
	Plan                                   *model.Plan
	Plans                                  model.PlanPage
	Recommendations                        model.RecommendationPage
	Recommendation                         *model.Recommendation
	Commands                               model.CommandPage
	FormCommands                           model.CommandPage
	Command                                *model.Command
	Actions                                model.ActionPage
	Action                                 model.ActionDetails
	Selected, Kind, PlanID, Next, FormNext string
}

func NewHandler(p service.PlannerService, m service.MigrationService, forms service.FormCommands, auth identity.AuthClient, a access.Authorizer) http.Handler {
	t := template.Must(template.New("").Funcs(templateFuncs()).ParseFS(templates, "templates/*.html"))
	h := &handler{p, m, forms, auth, a, t}
	root := http.NewServeMux()
	root.HandleFunc("GET /admin/login", h.login)
	root.HandleFunc("POST /admin/login", h.login)
	root.HandleFunc("POST /admin/logout", h.logout)
	pages := http.NewServeMux()
	pages.HandleFunc("GET /admin/{$}", h.status)
	pages.HandleFunc("GET /admin", h.status)
	pages.HandleFunc("GET /admin/status", h.status)
	pages.HandleFunc("GET /admin/models", h.models)
	pages.HandleFunc("GET /admin/settings", h.settings)
	pages.HandleFunc("POST /admin/settings/{operation}", h.update)
	pages.HandleFunc("POST /admin/commands/{operation}", h.command)
	pages.HandleFunc("GET /admin/plans", h.plans)
	pages.HandleFunc("GET /admin/recommendations", h.recommendations)
	pages.HandleFunc("GET /admin/recommendations/{id}", h.recommendation)
	pages.HandleFunc("GET /admin/commands", h.commands)
	pages.HandleFunc("GET /admin/commands/{id}", h.getCommand)
	pages.HandleFunc("GET /admin/migrations", h.actions)
	pages.HandleFunc("GET /admin/migrations/{id}", h.action)
	root.Handle("/admin/", h.protect(pages))
	root.Handle("/admin", h.protect(pages))
	return root
}
func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	st, err := h.planner.Status(r.Context())
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.render(w, r, 200, "status", view{Status: st})
}
func (h *handler) settings(w http.ResponseWriter, r *http.Request) {
	s, err := h.planner.GetSettings(r.Context())
	if err != nil {
		h.failure(w, r, err)
		return
	}
	m, err := h.migration.GetSettings(r.Context())
	if err != nil {
		h.failure(w, r, err)
		return
	}
	b, err := h.migration.GetBudget(r.Context())
	if err != nil {
		h.failure(w, r, err)
		return
	}
	pins, err := h.migration.ListPins(r.Context())
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.render(w, r, 200, "settings", view{Settings: s, Migration: m, Budget: b, Pins: pins, Fields: settingFields(s)})
}
func (h *handler) plans(w http.ResponseWriter, r *http.Request) {
	p, err := h.planner.ListPlans(r.Context(), model.PlanFilter{PageSize: 50, PageToken: r.URL.Query().Get("page_token")})
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.render(w, r, 200, "plans", view{Plans: p})
}
func (h *handler) loadPlan(r *http.Request) (*model.Plan, error) {
	if id := r.URL.Query().Get("plan_id"); id != "" {
		if !validID(id) {
			return nil, model.ErrInvalidArgument
		}
		p, err := h.planner.GetPlan(r.Context(), id)
		return &p, err
	}
	return h.planner.LatestPlan(r.Context())
}
func (h *handler) recommendations(w http.ResponseWriter, r *http.Request) {
	p, err := h.loadPlan(r)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	v := view{Plan: p, Selected: r.URL.Query().Get("selected"), Kind: r.URL.Query().Get("kind")}
	if p != nil {
		f := model.RecommendationFilter{PlanID: p.PlanID, Kind: model.RecommendationKind(v.Kind), PageSize: 50, PageToken: r.URL.Query().Get("page_token")}
		if v.Selected != "" {
			b, err := strconv.ParseBool(v.Selected)
			if err != nil {
				h.failure(w, r, model.ErrInvalidArgument)
				return
			}
			f.Selected = &b
		}
		v.Recommendations, err = h.planner.ListRecommendations(r.Context(), f)
		if err != nil {
			h.failure(w, r, err)
			return
		}
		q := r.URL.Query()
		q.Set("plan_id", p.PlanID)
		q.Set("page_token", v.Recommendations.NextPageToken)
		v.Next = "/admin/recommendations?" + q.Encode()
	}
	h.render(w, r, 200, "recommendations", v)
}
func (h *handler) recommendation(w http.ResponseWriter, r *http.Request) {
	if !validID(r.PathValue("id")) {
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	p, err := h.loadPlan(r)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	if p == nil {
		h.failure(w, r, model.ErrPlanNotFound)
		return
	}
	f := model.RecommendationFilter{PlanID: p.PlanID, PageSize: 100}
	for {
		page, err := h.planner.ListRecommendations(r.Context(), f)
		if err != nil {
			h.failure(w, r, err)
			return
		}
		for _, rec := range page.Recommendations {
			if rec.RecommendationID == r.PathValue("id") {
				h.render(w, r, 200, "recommendation", view{Plan: p, Recommendation: &rec})
				return
			}
		}
		if page.NextPageToken == "" {
			break
		}
		f.PageToken = page.NextPageToken
	}
	h.failure(w, r, model.ErrPlanNotFound)
}
func (h *handler) commands(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("plan_id") != "" && !validID(q.Get("plan_id")) {
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	p, err := h.planner.ListCommands(r.Context(), model.CommandFilter{PageSize: 50, PageToken: q.Get("page_token"), PlanID: q.Get("plan_id")})
	if err != nil {
		h.failure(w, r, err)
		return
	}
	var f model.CommandPage
	if h.forms != nil {
		f, err = h.forms.List(r.Context(), q.Get("form_page_token"))
		if err != nil {
			h.failure(w, r, err)
			return
		}
	}
	next := url.Values{"page_token": {p.NextPageToken}, "plan_id": {q.Get("plan_id")}, "form_page_token": {q.Get("form_page_token")}}
	fn := r.URL.Query()
	fn.Set("form_page_token", f.NextPageToken)
	h.render(w, r, 200, "commands", view{Commands: p, FormCommands: f, Next: "/admin/commands?" + next.Encode(), FormNext: "/admin/commands?" + fn.Encode()})
}
func (h *handler) getCommand(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	c, err := h.planner.GetCommand(r.Context(), id)
	if errors.Is(err, model.ErrCommandNotFound) && h.forms != nil {
		c, err = h.forms.Get(r.Context(), id)
	}
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.render(w, r, http.StatusOK, "command", view{Command: &c, Code: c.Code, Message: Help(c.Code)})
}
func (h *handler) actions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("plan_id") != "" && !validID(q.Get("plan_id")) {
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	p, err := h.migration.ListActions(r.Context(), model.ActionFilter{PlanID: q.Get("plan_id"), Status: model.ActionStatus(q.Get("status")), PageSize: 50, PageToken: q.Get("page_token")})
	if err != nil {
		h.failure(w, r, err)
		return
	}
	q.Set("page_token", p.NextPageToken)
	h.render(w, r, 200, "actions", view{Actions: p, PlanID: r.URL.Query().Get("plan_id"), Next: "/admin/migrations?" + q.Encode()})
}
func (h *handler) action(w http.ResponseWriter, r *http.Request) {
	if !validID(r.PathValue("id")) {
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	a, err := h.migration.GetAction(r.Context(), r.PathValue("id"))
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.render(w, r, 200, "action", view{Action: a})
}
func (h *handler) command(w http.ResponseWriter, r *http.Request) {
	if err := parseForm(w, r); err != nil {
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	op := r.PathValue("operation")
	req := model.CommandRequest{CommandID: r.PostForm.Get("command_id"), Initiator: access.Initiator(r.Context()), Type: model.CommandType(op), PlanID: r.PostForm.Get("plan_id")}
	switch req.Type {
	case model.CommandConfirm:
		req.RecommendationIDs = r.PostForm["recommendation_id"]
		if len(req.RecommendationIDs) == 0 && r.PostForm.Get("all") != "true" {
			h.fail(w, r, 400, "EMPTY_SELECTION")
			return
		}
	case model.CommandReject, model.CommandCancelPending, model.CommandRecompute, model.CommandPause, model.CommandResume, model.CommandRetrain:
	default:
		h.failure(w, r, model.ErrInvalidArgument)
		return
	}
	cmd, err := h.planner.Execute(r.Context(), req)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	h.showCommand(w, r, cmd)
}
func (h *handler) showCommand(w http.ResponseWriter, r *http.Request, c model.Command) {
	code := 200
	if c.Status == model.CommandRejected {
		code = 409
	}
	h.render(w, r, code, "command", view{Command: &c, Code: c.Code, Message: Help(c.Code)})
}
func validID(s string) bool  { _, err := uuid.Parse(s); return err == nil }
func invalid(s string) error { return fmt.Errorf("%w: %s", model.ErrInvalidArgument, s) }
func (h *handler) failure(w http.ResponseWriter, r *http.Request, err error) {
	code, key := 503, "UPSTREAM_UNAVAILABLE"
	switch {
	case errors.Is(err, model.ErrInvalidArgument):
		code, key = 400, "INVALID_ARGUMENT"
	case errors.Is(err, model.ErrNoCapacity):
		code, key = 409, "CAPACITY_LIMIT"
	case errors.Is(err, model.ErrPlanStale), errors.Is(err, model.ErrStale):
		code, key = 409, "PLAN_STALE"
	case errors.Is(err, model.ErrHistoryIncomplete):
		code, key = 409, "HISTORY_INCOMPLETE"
	case errors.Is(err, model.ErrModelUnavailable):
		code, key = 503, "MODEL_UNAVAILABLE"
	case errors.Is(err, model.ErrAccessDenied):
		code, key = 403, "ACCESS_DENIED"
	case errors.Is(err, model.ErrCommandConflict), errors.Is(err, model.ErrPinExists):
		code, key = 409, "COMMAND_CONFLICT"
	case errors.Is(err, model.ErrPlanNotFound), errors.Is(err, model.ErrCommandNotFound), errors.Is(err, model.ErrActionNotFound), errors.Is(err, model.ErrPinNotFound):
		code, key = 404, "NOT_FOUND"
	}
	h.fail(w, r, code, key)
}
func (h *handler) fail(w http.ResponseWriter, r *http.Request, status int, code string) {
	h.render(w, r, status, "error", view{Code: code, Message: Help(code)})
}
func Help(code string) string {
	switch strings.ToUpper(code) {
	case "":
		return "Команда принята. Откройте журнал миграций для проверки фактического результата."
	case "PLAN_STALE":
		return "План устарел или уже закрыт. Откройте последнюю ревизию, при необходимости рассчитайте новый план и проверьте рекомендации."
	case "DEPENDENCY_REQUIRED":
		return "Для выбранного переноса сначала нужно освободить место. Добавьте перечисленные ниже предшествующие действия и подтвердите выбор новой командой."
	case "HISTORY_INCOMPLETE", "HISTORY_INCOMPLETE_PLAN":
		return "История неполна. Проверьте Ingestor и пропуски событий, затем рассчитайте план заново."
	case "ACCESS_DENIED":
		return "Нет действующего доступа администратора. Войдите с учётной записью с правом ADMIN на system:placement; при отказе обратитесь к администратору."
	case "BUDGET_EXHAUSTED", "BUDGET":
		return "Бюджет переноса исчерпан. Дождитесь нового окна или увеличьте бюджет и рассчитайте план заново."
	case "CAPACITY_LIMIT", "HOT_CAPACITY", "COLD_CAPACITY", "NO_CAPACITY":
		return "Не хватает ёмкости с учётом временных копий. Дождитесь очистки, освободите место или измените допустимую загрузку и пересчитайте план."
	case "MODEL_UNAVAILABLE":
		return "Прогноз недоступен. Проверьте метод прогноза и повторите расчёт."
	case "EMPTY_SELECTION":
		return "Выберите хотя бы одну рекомендацию либо используйте отдельную кнопку подтверждения всего плана."
	case "PINNED":
		return "Объект закреплён. Снимите закрепление в настройках, если перенос допустим, и пересчитайте план."
	case "MIN_INTERVAL":
		return "Минимальный интервал ещё не прошёл. Дождитесь его окончания или измените настройку."
	case "IN_PROGRESS":
		return "Версия уже переносится. Дождитесь завершения и очистки источника."
	case "GAIN_BELOW_THRESHOLD":
		return "Ожидаемый выигрыш ниже порога с учётом штрафа. Проверьте прогноз и коэффициенты, затем пересчитайте план."
	case "NO_FORECAST":
		return "Нет положительного прогноза спроса. Накопите чтения и повторите расчёт."
	case "PIN_EXISTS":
		return "Закрепление уже существует. Обновите страницу настроек; для замены удалите его и добавьте заново."
	case "PIN_NOT_FOUND", "NOT_FOUND":
		return "Запись не найдена. Вернитесь к списку и обновите страницу."
	case "INVALID_ARGUMENT":
		return "Проверьте поля формы: числа, UUID и обязательные значения. Исправьте данные и отправьте новую команду."
	case "COMMAND_CONFLICT":
		return "Этот command_id уже использован с другими полями. Откройте новую форму для новой команды."
	default:
		return "Сервис временно недоступен. Проверьте Auth, AuthZ, Metadata и БД SPPR, затем повторите запрос с тем же command_id."
	}
}

func (h *handler) models(w http.ResponseWriter, r *http.Request) {
	provider, ok := h.planner.(interface {
		ModelVersions(context.Context, time.Time) ([]model.ModelVersion, error)
	})
	if !ok {
		h.render(w, r, 200, "models", view{})
		return
	}
	var before time.Time
	if raw := r.URL.Query().Get("before"); raw != "" {
		var err error
		before, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			h.failure(w, r, model.ErrInvalidArgument)
			return
		}
	}
	rows, err := provider.ModelVersions(r.Context(), before)
	if err != nil {
		h.failure(w, r, err)
		return
	}
	next := ""
	if len(rows) == 100 {
		next = "/admin/models?before=" + url.QueryEscape(rows[len(rows)-1].CreatedAt.Format(time.RFC3339Nano))
	}
	h.render(w, r, 200, "models", view{Models: rows, Next: next})
}
