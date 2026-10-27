package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	objectService "github.com/alesplll/opens3-rebac/services/metadata/internal/service/object"
	"github.com/alesplll/opens3-rebac/services/metadata/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestDeleteObjectMeta(t *testing.T) {
	type args struct {
		ctx        context.Context
		bucketName string
		key        string
	}

	ctx := context.Background()

	const cleanupDelay = 10 * time.Minute
	wantObjectID := "object-uuid-1"
	wantBlobID := "blob-uuid-1"
	dbErr := errors.New("db error")

	// The real manager rolls back when the handler returns an error; the mock
	// returns that error so the test sees what would be rolled back.
	executingTx := func(mc *minimock.Controller) *mocks.TxManagerMock {
		m := mocks.NewTxManagerMock(mc)
		m.ReadCommittedMock.Set(func(ctx context.Context, f db.Handler) error {
			return f(ctx)
		})
		return m
	}

	tests := []struct {
		name              string
		args              args
		wantObjectID      string
		wantBlobID        string
		wantErr           error
		buildRepoMock     func(mc *minimock.Controller) *mocks.ObjectRepositoryMock
		buildCopyMock     func(mc *minimock.Controller) *mocks.CopyRepositoryMock
		buildProducerMock func(mc *minimock.Controller) *mocks.ProducerMock
	}{
		{
			name:         "success: blobs scheduled for cleanup, object deleted, event sent",
			args:         args{ctx: ctx, bucketName: "my-bucket", key: "photos/cat.jpg"},
			wantObjectID: wantObjectID,
			wantBlobID:   wantBlobID,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				m := mocks.NewObjectRepositoryMock(mc)
				m.LockForDeleteMock.Expect(minimock.AnyContext, "my-bucket", "photos/cat.jpg").Return(wantObjectID, nil)
				m.DeleteMock.Expect(minimock.AnyContext, "my-bucket", "photos/cat.jpg").Return(wantObjectID, wantBlobID, nil)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.InsertOrphansMock.Expect(minimock.AnyContext, wantObjectID, cleanupDelay).Return(nil)
				return m
			},
			buildProducerMock: func(mc *minimock.Controller) *mocks.ProducerMock {
				m := mocks.NewProducerMock(mc)
				m.SendMock.Inspect(func(_ context.Context, key, value []byte, _ kafka.PrettyDecoder) {
					require.Equal(t, wantObjectID, string(key))
					require.JSONEq(t, `{"object_id":"object-uuid-1","blob_id":"blob-uuid-1"}`, string(value))
				}).Return(nil)
				return m
			},
		},
		{
			name:    "error: object not found, nothing scheduled",
			args:    args{ctx: ctx, bucketName: "my-bucket", key: "nonexistent.jpg"},
			wantErr: domainerrors.ErrObjectNotFound,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				m := mocks.NewObjectRepositoryMock(mc)
				m.LockForDeleteMock.Return("", domainerrors.ErrObjectNotFound)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				return mocks.NewCopyRepositoryMock(mc)
			},
			buildProducerMock: func(mc *minimock.Controller) *mocks.ProducerMock {
				return mocks.NewProducerMock(mc)
			},
		},
		{
			name:    "error: lock failure",
			args:    args{ctx: ctx, bucketName: "my-bucket", key: "photos/cat.jpg"},
			wantErr: dbErr,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				m := mocks.NewObjectRepositoryMock(mc)
				m.LockForDeleteMock.Return("", dbErr)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				return mocks.NewCopyRepositoryMock(mc)
			},
			buildProducerMock: func(mc *minimock.Controller) *mocks.ProducerMock {
				return mocks.NewProducerMock(mc)
			},
		},
		{
			name:    "error: scheduling blobs fails, object is not deleted",
			args:    args{ctx: ctx, bucketName: "my-bucket", key: "photos/cat.jpg"},
			wantErr: dbErr,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				m := mocks.NewObjectRepositoryMock(mc)
				m.LockForDeleteMock.Return(wantObjectID, nil)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.InsertOrphansMock.Return(dbErr)
				return m
			},
			buildProducerMock: func(mc *minimock.Controller) *mocks.ProducerMock {
				return mocks.NewProducerMock(mc)
			},
		},
		{
			name:    "error: delete fails after scheduling, no event",
			args:    args{ctx: ctx, bucketName: "my-bucket", key: "photos/cat.jpg"},
			wantErr: dbErr,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				m := mocks.NewObjectRepositoryMock(mc)
				m.LockForDeleteMock.Return(wantObjectID, nil)
				m.DeleteMock.Return("", "", dbErr)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.InsertOrphansMock.Return(nil)
				return m
			},
			buildProducerMock: func(mc *minimock.Controller) *mocks.ProducerMock {
				return mocks.NewProducerMock(mc)
			},
		},
		{
			name:    "error: producer failure",
			args:    args{ctx: ctx, bucketName: "my-bucket", key: "photos/cat.jpg"},
			wantErr: domainerrors.ErrInternal,
			buildRepoMock: func(mc *minimock.Controller) *mocks.ObjectRepositoryMock {
				m := mocks.NewObjectRepositoryMock(mc)
				m.LockForDeleteMock.Return(wantObjectID, nil)
				m.DeleteMock.Return(wantObjectID, wantBlobID, nil)
				return m
			},
			buildCopyMock: func(mc *minimock.Controller) *mocks.CopyRepositoryMock {
				m := mocks.NewCopyRepositoryMock(mc)
				m.InsertOrphansMock.Return(nil)
				return m
			},
			buildProducerMock: func(mc *minimock.Controller) *mocks.ProducerMock {
				m := mocks.NewProducerMock(mc)
				m.SendMock.Return(errors.New("kafka send failed"))
				return m
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)

			svc := objectService.NewService(
				tt.buildRepoMock(mc),
				mocks.NewPlacementRepositoryMock(mc),
				tt.buildCopyMock(mc),
				executingTx(mc),
				tt.buildProducerMock(mc),
				cleanupDelay,
				nil,
				nil,
			)

			objectID, blobID, err := svc.DeleteObjectMeta(tt.args.ctx, tt.args.bucketName, tt.args.key)

			require.Equal(t, tt.wantErr, err)
			if tt.wantErr == nil {
				require.Equal(t, tt.wantObjectID, objectID)
				require.Equal(t, tt.wantBlobID, blobID)
			}
		})
	}
}
