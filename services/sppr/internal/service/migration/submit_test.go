package migration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/migration"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/gojuno/minimock/v3"
	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/require"
)

const (
	planID  = "11111111-1111-4111-8111-111111111111"
	actionA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	actionB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	actionC = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	version = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
)

func spec(id string, deps ...string) model.ActionSpec {
	return model.ActionSpec{ActionID: id, VersionID: version, ExpectedGeneration: 1,
		SourceClass: model.StorageClassCold, TargetClass: model.StorageClassHot, SizeBytes: 10, DependsOn: deps}
}

func accepted(s model.ActionSpec, status model.ActionStatus) *model.Action {
	s.PlanID = planID
	if s.DependsOn == nil {
		s.DependsOn = []string{}
	}
	return &model.Action{ActionSpec: s, Status: status}
}

func withPlan(specs ...model.ActionSpec) []model.ActionSpec {
	out := make([]model.ActionSpec, 0, len(specs))
	for _, s := range specs {
		s.PlanID = planID
		if s.DependsOn == nil {
			s.DependsOn = []string{}
		}
		out = append(out, s)
	}
	return out
}

func executingTx(mc *minimock.Controller) *mocks.TxManagerMock {
	tx := mocks.NewTxManagerMock(mc)
	tx.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error { return f(ctx) })
	return tx
}

func TestSubmitActions(t *testing.T) {
	ctx := context.Background()
	other := spec(actionA)
	other.SizeBytes = 99
	cases := []struct {
		name    string
		planID  string
		specs   []model.ActionSpec
		build   func(*minimock.Controller) *mocks.ActionRepositoryMock
		want    model.SubmitResult
		wantErr error
	}{
		{
			name:   "accepts new actions with dependencies inside the request",
			planID: planID,
			specs:  []model.ActionSpec{spec(actionA), spec(actionB, actionA)},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.GetManyMock.Expect(minimock.AnyContext, []string{actionA, actionB}).Return(map[string]*model.Action{}, nil)
				r.InsertMock.Expect(minimock.AnyContext, withPlan(spec(actionA), spec(actionB, actionA))).Return(nil)
				return r
			},
			want: model.SubmitResult{Accepted: []string{actionA, actionB}, Existing: []string{}},
		},
		{
			name:   "canonicalizes UUIDs",
			planID: "11111111-1111-4111-8111-111111111111",
			specs:  []model.ActionSpec{spec("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA")},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.GetManyMock.Return(map[string]*model.Action{}, nil)
				r.InsertMock.Expect(minimock.AnyContext, withPlan(spec(actionA))).Return(nil)
				return r
			},
			want: model.SubmitResult{Accepted: []string{actionA}, Existing: []string{}},
		},
		{
			name:   "resubmission with the same fields is a no-op",
			planID: planID,
			specs:  []model.ActionSpec{spec(actionA)},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.GetManyMock.Return(map[string]*model.Action{actionA: accepted(spec(actionA), model.ActionRunning)}, nil)
				return r
			},
			want: model.SubmitResult{Accepted: []string{}, Existing: []string{actionA}},
		},
		{
			name:   "resubmission with other fields conflicts",
			planID: planID,
			specs:  []model.ActionSpec{spec(actionB), other},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.GetManyMock.Return(map[string]*model.Action{actionA: accepted(spec(actionA), model.ActionPending)}, nil)
				return r
			},
			wantErr: model.ErrActionConflict,
		},
		{
			name:   "depends on an accepted action of the plan",
			planID: planID,
			specs:  []model.ActionSpec{spec(actionB, actionA)},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.GetManyMock.Expect(minimock.AnyContext, []string{actionB, actionA}).
					Return(map[string]*model.Action{actionA: accepted(spec(actionA), model.ActionRunning)}, nil)
				r.InsertMock.Return(nil)
				return r
			},
			want: model.SubmitResult{Accepted: []string{actionB}, Existing: []string{}},
		},
		{
			name:   "dependency that already failed cancels the new action",
			planID: planID,
			specs:  []model.ActionSpec{spec(actionB, actionA)},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.GetManyMock.Return(map[string]*model.Action{actionA: accepted(spec(actionA), model.ActionFailed)}, nil)
				r.InsertMock.Return(nil)
				r.CancelDependentsMock.Expect(minimock.AnyContext, []string{actionA}).Return([]string{actionB}, nil)
				return r
			},
			want: model.SubmitResult{Accepted: []string{actionB}, Existing: []string{}},
		},
		{
			name:   "unknown dependency is rejected",
			planID: planID,
			specs:  []model.ActionSpec{spec(actionB, actionC)},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.GetManyMock.Return(map[string]*model.Action{}, nil)
				return r
			},
			wantErr: model.ErrInvalidArgument,
		},
		{
			name:   "dependency of another plan is rejected",
			planID: planID,
			specs:  []model.ActionSpec{spec(actionB, actionA)},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				foreign := accepted(spec(actionA), model.ActionPending)
				foreign.PlanID = "22222222-2222-4222-8222-222222222222"
				r.GetManyMock.Return(map[string]*model.Action{actionA: foreign}, nil)
				return r
			},
			wantErr: model.ErrInvalidArgument,
		},
		{
			name:   "concurrent submit of the same action conflicts",
			planID: planID,
			specs:  []model.ActionSpec{spec(actionA)},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.GetManyMock.Return(map[string]*model.Action{}, nil)
				r.InsertMock.Return(&pgconn.PgError{Code: "23505"})
				return r
			},
			wantErr: model.ErrActionConflict,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			s := migration.NewService(tt.build(mc), mocks.NewSettingsRepositoryMock(mc), mocks.NewPinRepositoryMock(mc), executingTx(mc), nil)
			got, err := s.SubmitActions(ctx, tt.planID, tt.specs)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSubmitActionsValidation(t *testing.T) {
	badClass := spec(actionA)
	badClass.TargetClass = "WARM"
	sameClass := spec(actionA)
	sameClass.TargetClass = model.StorageClassCold
	noGeneration := spec(actionA)
	noGeneration.ExpectedGeneration = 0
	negative := spec(actionA)
	negative.SizeBytes = -1
	badVersion := spec(actionA)
	badVersion.VersionID = "v1"
	cases := []struct {
		name   string
		planID string
		specs  []model.ActionSpec
	}{
		{"plan_id is not a UUID", "plan", []model.ActionSpec{spec(actionA)}},
		{"empty request", planID, nil},
		{"action_id is not a UUID", planID, []model.ActionSpec{spec("a")}},
		{"version_id is not a UUID", planID, []model.ActionSpec{badVersion}},
		{"repeated action_id", planID, []model.ActionSpec{spec(actionA), spec(actionA)}},
		{"unknown class", planID, []model.ActionSpec{badClass}},
		{"same source and target", planID, []model.ActionSpec{sameClass}},
		{"non-positive generation", planID, []model.ActionSpec{noGeneration}},
		{"negative size", planID, []model.ActionSpec{negative}},
		{"depends on itself", planID, []model.ActionSpec{spec(actionA, actionA)}},
		{"cycle", planID, []model.ActionSpec{spec(actionA, actionC), spec(actionB, actionA), spec(actionC, actionB)}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			repo := mocks.NewActionRepositoryMock(mc)
			tx := mocks.NewTxManagerMock(mc)
			s := migration.NewService(repo, mocks.NewSettingsRepositoryMock(mc), mocks.NewPinRepositoryMock(mc), tx, nil)
			_, err := s.SubmitActions(context.Background(), tt.planID, tt.specs)
			require.ErrorIs(t, err, model.ErrInvalidArgument)
			require.Equal(t, uint64(0), tx.ReadCommittedAfterCounter())
		})
	}
}

func TestCancelActions(t *testing.T) {
	repoErr := errors.New("database unavailable")
	cases := []struct {
		name    string
		req     model.CancelRequest
		build   func(*minimock.Controller) *mocks.ActionRepositoryMock
		want    model.CancelResult
		wantErr error
	}{
		{
			name: "cancels a plan and its dependents",
			req:  model.CancelRequest{PlanID: planID},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.CancelPendingMock.Expect(minimock.AnyContext, model.CancelRequest{PlanID: planID, ActionIDs: []string{}}).Return([]string{actionA}, nil)
				r.CancelDependentsMock.Expect(minimock.AnyContext, []string{actionA}).Return([]string{actionB}, nil)
				return r
			},
			want: model.CancelResult{Cancelled: []string{actionA}, Dependents: []string{actionB}},
		},
		{
			name: "nothing pending cancels nothing",
			req:  model.CancelRequest{ActionIDs: []string{actionA}},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.CancelPendingMock.Return([]string{}, nil)
				return r
			},
			want: model.CancelResult{Cancelled: []string{}, Dependents: []string{}},
		},
		{
			name: "repository error",
			req:  model.CancelRequest{ActionIDs: []string{actionA}},
			build: func(mc *minimock.Controller) *mocks.ActionRepositoryMock {
				r := mocks.NewActionRepositoryMock(mc)
				r.CancelPendingMock.Return(nil, repoErr)
				return r
			},
			wantErr: repoErr,
		},
		{
			name:    "both plan and ids",
			req:     model.CancelRequest{PlanID: planID, ActionIDs: []string{actionA}},
			build:   func(mc *minimock.Controller) *mocks.ActionRepositoryMock { return mocks.NewActionRepositoryMock(mc) },
			wantErr: model.ErrInvalidArgument,
		},
		{
			name:    "neither plan nor ids",
			build:   func(mc *minimock.Controller) *mocks.ActionRepositoryMock { return mocks.NewActionRepositoryMock(mc) },
			wantErr: model.ErrInvalidArgument,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			tx := mocks.NewTxManagerMock(mc)
			tx.ReadCommittedMock.Optional().Set(func(ctx context.Context, f db.Handler) error { return f(ctx) })
			s := migration.NewService(tt.build(mc), mocks.NewSettingsRepositoryMock(mc), mocks.NewPinRepositoryMock(mc), tx, nil)
			got, err := s.CancelActions(context.Background(), tt.req)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
