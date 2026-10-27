package cleanup_test

import (
	"context"
	"errors"
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
		build   func(*mocks.PlacementClientMock, *mocks.StorageClientMock)
		wantErr error
	}{
		{name: "source deleted", build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock) {
			p.ListDueCleanupsMock.Expect(minimock.AnyContext, int32(100)).Return([]model.PlacementCopy{copy}, nil)
			s.DeleteMock.Expect(minimock.AnyContext, "blob").Return(nil)
			p.CompleteCleanupMock.Expect(minimock.AnyContext, "copy", true, "").Return(nil)
		}},
		{name: "deletion failure recorded", wantErr: storageErr, build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock) {
			p.ListDueCleanupsMock.Return([]model.PlacementCopy{copy}, nil)
			s.DeleteMock.Return(storageErr)
			p.CompleteCleanupMock.Expect(minimock.AnyContext, "copy", false, storageErr.Error()).Return(nil)
		}},
		{name: "completion failure remains retryable", wantErr: metadataErr, build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock) {
			p.ListDueCleanupsMock.Return([]model.PlacementCopy{copy}, nil)
			s.DeleteMock.Return(nil)
			p.CompleteCleanupMock.Expect(minimock.AnyContext, "copy", true, "").Return(metadataErr)
		}},
		{name: "list failure never deletes", wantErr: metadataErr, build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock) {
			p.ListDueCleanupsMock.Return(nil, metadataErr)
		}},
		{name: "continues through roles after failure", wantErr: storageErr, build: func(p *mocks.PlacementClientMock, s *mocks.StorageClientMock) {
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
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			p := mocks.NewPlacementClientMock(mc)
			s := mocks.NewStorageClientMock(mc)
			tt.build(p, s)
			c := cleanup.NewService(time.Second, p, map[model.StorageClass]client.StorageClient{model.StorageClassHot: s}, nil)
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
	c := cleanup.NewService(time.Hour, p, map[model.StorageClass]client.StorageClient{model.StorageClassCold: s}, nil)
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
	c := cleanup.NewService(time.Second, p, map[model.StorageClass]client.StorageClient{model.StorageClassCold: s}, nil)
	require.ErrorContains(t, c.Step(context.Background()), "lost reply")
	require.NoError(t, c.Step(context.Background()))
}
