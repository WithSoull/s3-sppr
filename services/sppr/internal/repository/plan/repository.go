package plan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/contextx/txctx"
	"github.com/jackc/pgx/v4"
)

type repo struct{ db db.Client }

func NewRepository(client db.Client) repository.PlanRepository { return &repo{db: client} }

func requireTx(ctx context.Context, op string) error {
	if _, ok := txctx.ExtractTx(ctx); !ok {
		return fmt.Errorf("%s requires a transaction", op)
	}
	return nil
}

const selectPlans = `
SELECT plan_id::text, revision, trigger, status, method_version, params, summary, reason, created_at, finished_at
FROM plans`

type planRow struct {
	PlanID        string     `db:"plan_id"`
	Revision      int64      `db:"revision"`
	Trigger       string     `db:"trigger"`
	Status        string     `db:"status"`
	MethodVersion string     `db:"method_version"`
	Params        []byte     `db:"params"`
	Summary       []byte     `db:"summary"`
	Reason        string     `db:"reason"`
	CreatedAt     time.Time  `db:"created_at"`
	FinishedAt    *time.Time `db:"finished_at"`
}

func (r planRow) toModel() (model.Plan, error) {
	p := model.Plan{
		PlanID: r.PlanID, Revision: r.Revision, Trigger: model.PlanTrigger(r.Trigger), Status: model.PlanStatus(r.Status),
		MethodVersion: r.MethodVersion, Reason: r.Reason, CreatedAt: r.CreatedAt.UTC(),
	}
	if r.FinishedAt != nil {
		t := r.FinishedAt.UTC()
		p.FinishedAt = &t
	}
	if err := json.Unmarshal(r.Params, &p.Params); err != nil {
		return model.Plan{}, fmt.Errorf("decode params of plan %s: %w", r.PlanID, err)
	}
	if err := json.Unmarshal(r.Summary, &p.Summary); err != nil {
		return model.Plan{}, fmt.Errorf("decode summary of plan %s: %w", r.PlanID, err)
	}
	return p, nil
}

func toPlans(rows []planRow) ([]model.Plan, error) {
	out := make([]model.Plan, 0, len(rows))
	for _, row := range rows {
		p, err := row.toModel()
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func (r *repo) CreatePlan(ctx context.Context, p model.Plan) (model.Plan, error) {
	params, err := json.Marshal(p.Params)
	if err != nil {
		return model.Plan{}, err
	}
	var rows []planRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "plan:create", QueryRaw: `
		INSERT INTO plans (plan_id, trigger, status, method_version, params)
		VALUES ($1::uuid, $2, 'computing', $3, $4)
		RETURNING plan_id::text, revision, trigger, status, method_version, params, summary, reason, created_at, finished_at`},
		p.PlanID, string(p.Trigger), p.MethodVersion, params); err != nil {
		return model.Plan{}, err
	}
	return rows[0].toModel()
}

func (r *repo) FinishPlan(ctx context.Context, p model.Plan, recs []model.Recommendation) error {
	if err := requireTx(ctx, "plan finish"); err != nil {
		return err
	}
	summary, err := json.Marshal(p.Summary)
	if err != nil {
		return err
	}
	tag, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:finish", QueryRaw: `
		UPDATE plans SET status = $2, summary = $3, reason = $4, method_version = $5, finished_at = now()
		WHERE plan_id = $1::uuid AND status = 'computing'`},
		p.PlanID, string(p.Status), summary, p.Reason, p.MethodVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("plan %s is not computing", p.PlanID)
	}
	if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:supersede", QueryRaw: `
		UPDATE plans SET status = 'superseded'
		WHERE status = 'ready' AND revision < (SELECT revision FROM plans WHERE plan_id = $1::uuid)`}, p.PlanID); err != nil {
		return err
	}
	return r.insertRecommendations(ctx, p.PlanID, recs)
}

// insertRecommendations writes the plan in one statement per table: a plan
// with thousands of rejected candidates would otherwise take thousands of
// round trips inside the transaction.
func (r *repo) insertRecommendations(ctx context.Context, planID string, recs []model.Recommendation) error {
	if len(recs) == 0 {
		return nil
	}
	n := len(recs)
	ids, kinds, reasons, versions := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	buckets, keys, sources, targets := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	orders := make([]int32, n)
	selected := make([]bool, n)
	generations, sizes := make([]int64, n), make([]int64, n)
	explanations := make([]string, n)
	var depFrom, depTo []string
	for i, rec := range recs {
		e, err := json.Marshal(rec.Explanation)
		if err != nil {
			return err
		}
		ids[i], orders[i], kinds[i], selected[i], reasons[i] = rec.RecommendationID, int32(rec.Order), string(rec.Kind), rec.Selected, string(rec.Reason)
		versions[i], buckets[i], keys[i], generations[i] = rec.VersionID, rec.Bucket, rec.Key, rec.ExpectedGeneration
		sources[i], targets[i], sizes[i], explanations[i] = string(rec.SourceClass), string(rec.TargetClass), rec.SizeBytes, string(e)
		for _, d := range rec.DependsOn {
			depFrom = append(depFrom, rec.RecommendationID)
			depTo = append(depTo, d)
		}
	}
	if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:insert_recommendations", QueryRaw: `
		INSERT INTO recommendations (recommendation_id, plan_id, ord, kind, selected, reason, version_id, bucket,
		                             object_key, expected_generation, source_class, target_class, size_bytes, explanation)
		SELECT u.id::uuid, $1::uuid, u.ord, u.kind, u.selected, u.reason, u.version::uuid, u.bucket,
		       u.key, u.generation, u.source, u.target, u.size, u.explanation::jsonb
		FROM unnest($2::text[], $3::int[], $4::text[], $5::bool[], $6::text[], $7::text[], $8::text[],
		            $9::text[], $10::bigint[], $11::text[], $12::text[], $13::bigint[], $14::text[])
		     AS u(id, ord, kind, selected, reason, version, bucket, key, generation, source, target, size, explanation)`},
		planID, ids, orders, kinds, selected, reasons, versions, buckets, keys, generations, sources, targets, sizes, explanations); err != nil {
		return err
	}
	if len(depFrom) == 0 {
		return nil
	}
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:insert_recommendation_deps", QueryRaw: `
		INSERT INTO recommendation_deps (recommendation_id, depends_on)
		SELECT f::uuid, t::uuid FROM unnest($1::text[], $2::text[]) AS u(f, t)`}, depFrom, depTo)
	return err
}

func (r *repo) FailInterrupted(ctx context.Context) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:fail_interrupted", QueryRaw: `
		UPDATE plans SET status = 'failed', reason = 'interrupted by a restart', finished_at = now()
		WHERE status = 'computing'`})
	return err
}

func (r *repo) GetPlan(ctx context.Context, planID string, forUpdate bool) (model.Plan, error) {
	q := db.Query{Name: "plan:get", QueryRaw: selectPlans + ` WHERE plan_id = $1::uuid`}
	if forUpdate {
		if err := requireTx(ctx, "plan lock"); err != nil {
			return model.Plan{}, err
		}
		q = db.Query{Name: "plan:get_for_update", QueryRaw: q.QueryRaw + ` FOR UPDATE`}
	}
	var rows []planRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, q, planID); err != nil {
		return model.Plan{}, err
	}
	if len(rows) == 0 {
		return model.Plan{}, model.ErrPlanNotFound
	}
	return rows[0].toModel()
}

func (r *repo) LatestPlan(ctx context.Context) (*model.Plan, error) {
	var rows []planRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "plan:latest", QueryRaw: selectPlans + `
		WHERE status <> 'computing' ORDER BY revision DESC LIMIT 1`}); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	p, err := rows[0].toModel()
	return &p, err
}

func (r *repo) ListPlans(ctx context.Context, f model.PlanFilter) ([]model.Plan, error) {
	var before *int64
	if f.PageToken != "" {
		rev, err := strconv.ParseInt(f.PageToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: page_token", model.ErrInvalidArgument)
		}
		before = &rev
	}
	var rows []planRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "plan:list", QueryRaw: selectPlans + `
		WHERE ($1::bigint IS NULL OR revision < $1) ORDER BY revision DESC LIMIT $2`}, before, f.PageSize); err != nil {
		return nil, err
	}
	return toPlans(rows)
}

func (r *repo) SetPlanStatus(ctx context.Context, planID string, status model.PlanStatus) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:set_status", QueryRaw: `
		UPDATE plans SET status = $2 WHERE plan_id = $1::uuid`}, planID, string(status))
	return err
}

func (r *repo) PrunePlans(ctx context.Context, keep int) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:prune", QueryRaw: `
		DELETE FROM plans WHERE plan_id IN (
			SELECT plan_id FROM plans
			WHERE status IN ('ready','superseded','history_incomplete','failed','rejected')
			ORDER BY revision DESC OFFSET $1)`}, keep)
	return err
}

const selectRecommendations = `
SELECT r.recommendation_id::text, r.plan_id::text, r.ord, r.kind, r.selected, r.reason, r.version_id::text,
       r.bucket, r.object_key, r.expected_generation, r.source_class, r.target_class, r.size_bytes, r.explanation,
       ARRAY(SELECT d.depends_on::text FROM recommendation_deps d
             WHERE d.recommendation_id = r.recommendation_id ORDER BY d.depends_on) AS depends_on
FROM recommendations r`

type recommendationRow struct {
	RecommendationID   string   `db:"recommendation_id"`
	PlanID             string   `db:"plan_id"`
	Ord                int32    `db:"ord"`
	Kind               string   `db:"kind"`
	Selected           bool     `db:"selected"`
	Reason             string   `db:"reason"`
	VersionID          string   `db:"version_id"`
	Bucket             string   `db:"bucket"`
	ObjectKey          string   `db:"object_key"`
	ExpectedGeneration int64    `db:"expected_generation"`
	SourceClass        string   `db:"source_class"`
	TargetClass        string   `db:"target_class"`
	SizeBytes          int64    `db:"size_bytes"`
	Explanation        []byte   `db:"explanation"`
	DependsOn          []string `db:"depends_on"`
}

func toRecommendations(rows []recommendationRow) ([]model.Recommendation, error) {
	out := make([]model.Recommendation, 0, len(rows))
	for _, row := range rows {
		rec := model.Recommendation{
			RecommendationID: row.RecommendationID, PlanID: row.PlanID, Order: int(row.Ord),
			Kind: model.RecommendationKind(row.Kind), Selected: row.Selected, Reason: model.RejectReason(row.Reason),
			VersionID: row.VersionID, Bucket: row.Bucket, Key: row.ObjectKey, ExpectedGeneration: row.ExpectedGeneration,
			SourceClass: model.StorageClass(row.SourceClass), TargetClass: model.StorageClass(row.TargetClass),
			SizeBytes: row.SizeBytes, DependsOn: row.DependsOn,
		}
		if len(rec.DependsOn) == 0 {
			rec.DependsOn = nil
		}
		if err := json.Unmarshal(row.Explanation, &rec.Explanation); err != nil {
			return nil, fmt.Errorf("decode explanation of %s: %w", row.RecommendationID, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

func (r *repo) ListRecommendations(ctx context.Context, f model.RecommendationFilter) ([]model.Recommendation, error) {
	after := int64(-1)
	if f.PageToken != "" {
		ord, err := strconv.ParseInt(f.PageToken, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%w: page_token", model.ErrInvalidArgument)
		}
		after = ord
	}
	var rows []recommendationRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "plan:list_recommendations", QueryRaw: selectRecommendations + `
		WHERE r.plan_id = $1::uuid AND r.ord > $2
		  AND ($3::bool IS NULL OR r.selected = $3) AND ($4::text IS NULL OR r.kind = $4)
		ORDER BY r.ord LIMIT $5`}, f.PlanID, after, f.Selected, nullable(string(f.Kind)), f.PageSize); err != nil {
		return nil, err
	}
	return toRecommendations(rows)
}

func (r *repo) SelectedRecommendations(ctx context.Context, planID string) ([]model.Recommendation, error) {
	var rows []recommendationRow
	if err := r.db.DB().ScanAllContext(ctx, &rows, db.Query{Name: "plan:selected_recommendations", QueryRaw: selectRecommendations + `
		WHERE r.plan_id = $1::uuid AND r.selected ORDER BY r.ord`}, planID); err != nil {
		return nil, err
	}
	return toRecommendations(rows)
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

var errNoSettings = errors.New("planner settings are not initialized")

func (r *repo) EnsureSettings(ctx context.Context, s model.PlannerSettings) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:settings_defaults", QueryRaw: `
		INSERT INTO planner_settings (horizon_seconds, hot_fixed_ms, hot_mib_per_ms, cold_fixed_ms, cold_mib_per_ms,
		                              penalty_ms_per_mib, min_gain_ms, hot_utilization, cold_utilization)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO NOTHING`}, settingsArgs(s)...)
	return err
}

func (r *repo) GetSettings(ctx context.Context) (model.PlannerSettings, error) {
	var s model.PlannerSettings
	var horizon int64
	err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "plan:settings_get", QueryRaw: `
		SELECT horizon_seconds, hot_fixed_ms, hot_mib_per_ms, cold_fixed_ms, cold_mib_per_ms,
		       penalty_ms_per_mib, min_gain_ms, hot_utilization, cold_utilization FROM planner_settings`}).
		Scan(&horizon, &s.HotFixedMs, &s.HotMiBPerMs, &s.ColdFixedMs, &s.ColdMiBPerMs,
			&s.PenaltyMsPerMiB, &s.MinGainMs, &s.HotUtilization, &s.ColdUtilization)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.PlannerSettings{}, errNoSettings
	}
	s.Horizon = time.Duration(horizon) * time.Second
	return s, err
}

func (r *repo) UpdateSettings(ctx context.Context, s model.PlannerSettings) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "plan:settings_update", QueryRaw: `
		UPDATE planner_settings SET horizon_seconds = $1, hot_fixed_ms = $2, hot_mib_per_ms = $3, cold_fixed_ms = $4,
		       cold_mib_per_ms = $5, penalty_ms_per_mib = $6, min_gain_ms = $7, hot_utilization = $8,
		       cold_utilization = $9, updated_at = now()`}, settingsArgs(s)...)
	return err
}

func settingsArgs(s model.PlannerSettings) []any {
	return []any{int64(s.Horizon / time.Second), s.HotFixedMs, s.HotMiBPerMs, s.ColdFixedMs, s.ColdMiBPerMs,
		s.PenaltyMsPerMiB, s.MinGainMs, s.HotUtilization, s.ColdUtilization}
}
