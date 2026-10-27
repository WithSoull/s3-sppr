package cleanup_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/cleanup"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestStep(t *testing.T) {
	logger.SetNopLogger()
	storageErr, metadataErr := errors.New("Storage unavailable"), errors.New("Metadata unavailable")
	copy := model.PlacementCopy{CopyID: "copy", BlobID: "blob", Class: model.StorageClassHot, Role: model.CopyRoleSource}
	cases := []struct {
		name    string
		build   func(*mocks.PlacementClientMock, *mocks.StorageClientMock, *mocks.ActionRepositoryMock)
		wantErr error
	}{
		{name: "source deleted", build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock, a *mocks.ActionRepositoryMock) {
			p.ListDueCleanupsMock.Expect(minimock.AnyContext, int32(100)).Return([]model.PlacementCopy{copy}, nil)
			s.DeleteMock.Expect(minimock.AnyContext, "blob").Return(nil)
			p.CompleteCleanupMock.Expect(minimock.AnyContext, "copy", true, "").Return(nil)
			a.ListAwaitingSourceCleanupMock.Expect(minimock.AnyContext, 100).Return(nil, nil)
		}},
		{name: "deletion failure recorded", wantErr: storageErr, build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock, a *mocks.ActionRepositoryMock) {
			p.ListDueCleanupsMock.Return([]model.PlacementCopy{copy}, nil)
			s.DeleteMock.Return(storageErr)
			p.CompleteCleanupMock.Expect(minimock.AnyContext, "copy", false, storageErr.Error()).Return(nil)
			a.ListAwaitingSourceCleanupMock.Return(nil, nil)
		}},
		{name: "completion failure remains retryable", wantErr: metadataErr, build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock, a *mocks.ActionRepositoryMock) {
			p.ListDueCleanupsMock.Return([]model.PlacementCopy{copy}, nil)
			s.DeleteMock.Return(nil)
			p.CompleteCleanupMock.Expect(minimock.AnyContext, "copy", true, "").Return(metadataErr)
			a.ListAwaitingSourceCleanupMock.Return(nil, nil)
		}},
		{name: "list failure never deletes", wantErr: metadataErr, build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock, a *mocks.ActionRepositoryMock) {
			p.ListDueCleanupsMock.Return(nil, metadataErr)
		}},
		{name: "continues through roles after failure", wantErr: storageErr, build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock, a *mocks.ActionRepositoryMock) {
			orphan := copy
			orphan.CopyID = "orphan"
			orphan.BlobID = "orphan-blob"
			orphan.Role = model.CopyRoleOrphan
			target := copy
			target.CopyID = "target"
			target.BlobID = "target-blob"
			target.Role = model.CopyRoleTarget
			p.ListDueCleanupsMock.Return([]model.PlacementCopy{copy, orphan, target}, nil)
			s.DeleteMock.When(minimock.AnyContext, "blob").Then(storageErr)
			s.DeleteMock.When(minimock.AnyContext, "orphan-blob").Then(nil)
			s.DeleteMock.When(minimock.AnyContext, "target-blob").Then(nil)
			p.CompleteCleanupMock.When(minimock.AnyContext, "copy", false, storageErr.Error()).Then(nil)
			p.CompleteCleanupMock.When(minimock.AnyContext, "orphan", true, "").Then(nil)
			p.CompleteCleanupMock.When(minimock.AnyContext, "target", true, "").Then(nil)
			a.ListAwaitingSourceCleanupMock.Return(nil, nil)
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			p := mocks.NewPlacementClientMock(mc)
			s := mocks.NewStorageClientMock(mc)
			a := mocks.NewActionRepositoryMock(mc)
			tt.build(p, s, a)
			c := cleanup.NewService(time.Second, a, p, map[model.StorageClass]client.StorageClient{model.StorageClassHot: s}, nil)
			err := c.Step(context.Background())
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRunCancellationLeavesCopyDue(t *testing.T) {
	mc := minimock.NewController(t)
	p := mocks.NewPlacementClientMock(mc)
	s := mocks.NewStorageClientMock(mc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.ListDueCleanupsMock.Return([]model.PlacementCopy{{CopyID: "copy", BlobID: "blob", Class: model.StorageClassCold}}, nil)
	s.DeleteMock.Set(func(ctx context.Context, _ string) error { cancel(); return ctx.Err() })
	c := cleanup.NewService(time.Hour, mocks.NewActionRepositoryMock(mc), p, map[model.StorageClass]client.StorageClient{model.StorageClassCold: s}, nil)
	require.NoError(t, c.Run(ctx))
	require.Zero(t, p.CompleteCleanupAfterCounter())
}

func TestDeleteSucceededButCompletionLost(t *testing.T) {
	mc := minimock.NewController(t)
	p := mocks.NewPlacementClientMock(mc)
	s := mocks.NewStorageClientMock(mc)
	p.ListDueCleanupsMock.Times(2).Return([]model.PlacementCopy{{CopyID: "copy", BlobID: "blob", Class: model.StorageClassCold}}, nil)
	s.DeleteMock.Times(2).Return(nil)
	calls := 0
	p.CompleteCleanupMock.Set(func(_ context.Context, id string, deleted bool, message string) error {
		calls++
		require.Equal(t, "copy", id)
		require.True(t, deleted)
		require.Empty(t, message)
		if calls == 1 {
			return errors.New("lost reply")
		}
		return nil
	})
	a := mocks.NewActionRepositoryMock(mc)
	a.ListAwaitingSourceCleanupMock.Times(2).Return(nil, nil)
	c := cleanup.NewService(time.Second, a, p, map[model.StorageClass]client.StorageClient{model.StorageClassCold: s}, nil)
	require.ErrorContains(t, c.Step(context.Background()), "lost reply")
	require.NoError(t, c.Step(context.Background()))
}

func TestStepReleasesDependents(t *testing.T) {
	logger.SetNopLogger()
	listErr := errors.New("Metadata unavailable")
	source := func(state model.CopyState) model.PlacementCopy {
		return model.PlacementCopy{CopyID: "s", Role: model.CopyRoleSource, State: state}
	}
	target := model.PlacementCopy{CopyID: "t", Role: model.CopyRoleTarget, State: model.CopyPromoted}
	cases := []struct {
		name     string
		copies   map[string][]model.PlacementCopy
		listErr  map[string]error
		awaitErr error
		marked   []string
		wantErr  error
	}{
		{name: "deleted source releases", copies: map[string][]model.PlacementCopy{"a1": {target, source(model.CopyDeleted)}}, marked: []string{"a1"}},
		{name: "source still pending waits", copies: map[string][]model.PlacementCopy{"a1": {target, source(model.CopyCleanupPending)}}},
		{name: "failed source cleanup waits", copies: map[string][]model.PlacementCopy{"a1": {target, source(model.CopyCleanupFailed)}}},
		{name: "action without source copy releases", copies: map[string][]model.PlacementCopy{"a1": {target}}, marked: []string{"a1"}},
		{
			name:    "one Metadata failure does not stop the others",
			copies:  map[string][]model.PlacementCopy{"a2": {target, source(model.CopyDeleted)}},
			listErr: map[string]error{"a1": listErr},
			marked:  []string{"a2"},
			wantErr: listErr,
		},
		{name: "repository failure is reported", awaitErr: listErr, wantErr: listErr},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			p := mocks.NewPlacementClientMock(mc)
			a := mocks.NewActionRepositoryMock(mc)
			p.ListDueCleanupsMock.Return(nil, nil)
			var ids []string
			for id := range tt.listErr {
				ids = append(ids, id)
			}
			for id := range tt.copies {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			a.ListAwaitingSourceCleanupMock.Return(ids, tt.awaitErr)
			for _, id := range ids {
				p.ListMigrationCopiesMock.When(minimock.AnyContext, id).Then(tt.copies[id], tt.listErr[id])
			}
			var marked []string
			if len(tt.marked) > 0 {
				a.MarkSourceCleanedMock.Set(func(_ context.Context, id string) error {
					marked = append(marked, id)
					return nil
				})
			}
			c := cleanup.NewService(time.Second, a, p, nil, nil)
			err := c.Step(context.Background())
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.marked, marked)
		})
	}
}
