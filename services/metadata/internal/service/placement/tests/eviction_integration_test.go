//go:build integration

package tests

import (
	"context"
	"time"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/contextx/txctx"
	"github.com/jackc/pgx/v4"
)

func (s *PlacementProtocolSuite) TestNoncurrentHotEviction() {
	s.mustCreateBucket()
	old := s.mustUploadHot("a.txt", "v1")
	current := s.mustUpload("a.txt", "v2")
	oldBlob := s.placementBlob(old)
	migration := s.migrationID("evict")
	target := s.mustReserve(migration, old, 1, model.StorageClassHot, model.StorageClassCold)
	replay := s.mustReserve(migration, old, 1, model.StorageClassHot, model.StorageClassCold)
	s.Require().Equal(target.CopyID, replay.CopyID)

	switched, err := s.placements.SwitchPlacement(s.Context(), migration, objectSize, md5OfCopy, time.Hour)
	s.Require().NoError(err)
	s.Require().Equal(old, switched.VersionID)
	s.Require().Equal(int64(2), switched.NewGeneration)
	s.Require().Equal(model.StorageClassCold, switched.StorageClass)
	s.Require().Equal(target.BlobID, switched.BlobID)
	s.Require().Equal(model.CopyRoleSource, switched.SourceCopy.Role)
	s.Require().Equal(model.CopyStateCleanupPending, switched.SourceCopy.State)
	s.Require().Equal(model.StorageClassHot, switched.SourceCopy.StorageClass)
	s.Require().Equal(oldBlob, switched.SourceCopy.BlobID)
	s.Require().NotNil(switched.SourceCopy.CleanupAfter)

	replayed, err := s.placements.SwitchPlacement(s.Context(), migration, objectSize, md5OfCopy, time.Hour)
	s.Require().NoError(err)
	s.Require().Equal(switched, replayed)
	p, err := s.placements.GetPlacement(s.Context(), old)
	s.Require().NoError(err)
	s.Require().False(p.IsCurrent)
	s.Require().Equal(model.StorageClassCold, p.StorageClass)
	s.Require().Equal(target.BlobID, p.BlobID)
	now, err := s.objects.GetMeta(s.Context(), s.bucketName(), "a.txt", "")
	s.Require().NoError(err)
	s.Require().Equal(current, now.VersionID)
	s.Require().Equal(int64(objectSize), s.usage(model.StorageClassHot).CopyBytes)
	copies, err := s.placements.ListMigrationCopies(s.Context(), migration)
	s.Require().NoError(err)
	s.Require().Len(copies, 2)
	s.Require().Equal(model.CopyStatePromoted, copies[0].State)
	due, err := s.placements.ListDueCleanups(s.Context(), 10)
	s.Require().NoError(err)
	s.Require().Empty(due)
	s.expireCleanupDelays()
	_, err = s.placements.CompleteCleanup(s.Context(), switched.SourceCopy.CopyID, true, "")
	s.Require().NoError(err)
	s.Require().Equal(int64(0), s.usage(model.StorageClassHot).CopyBytes)
}

func (s *PlacementProtocolSuite) TestOverwriteDuringHotEviction() {
	s.mustCreateBucket()
	for _, initiallyCurrent := range []bool{false, true} {
		key := "old.txt"
		if initiallyCurrent {
			key = "current.txt"
		}
		old := s.mustUploadHot(key, "v1")
		if !initiallyCurrent {
			s.mustUpload(key, "v2")
		}
		migration := s.migrationID(key)
		target := s.mustReserve(migration, old, 1, model.StorageClassHot, model.StorageClassCold)
		newest := s.mustUpload(key, "newest")
		switched, err := s.placements.SwitchPlacement(s.Context(), migration, objectSize, md5OfCopy, time.Hour)
		s.Require().NoError(err)
		s.Require().Equal(target.BlobID, switched.BlobID)
		s.Require().Equal(model.CopyStateCleanupPending, switched.SourceCopy.State)
		p, err := s.placements.GetPlacement(s.Context(), old)
		s.Require().NoError(err)
		s.Require().False(p.IsCurrent)
		s.Require().Equal(model.StorageClassCold, p.StorageClass)
		current, err := s.objects.GetMeta(s.Context(), s.bucketName(), key, "")
		s.Require().NoError(err)
		s.Require().Equal(newest, current.VersionID)
	}
}

func (s *PlacementProtocolSuite) TestNoncurrentColdCannotBePromoted() {
	s.mustCreateBucket()
	old := s.mustUpload("a.txt", "v1")
	s.mustUpload("a.txt", "v2")
	_, err := s.placements.ReserveCopy(s.Context(), s.migrationID("promote"), old, 1, model.StorageClassCold, model.StorageClassHot)
	s.Require().ErrorIs(err, domainerrors.ErrVersionNotCurrent)
	s.Require().Equal(int64(0), s.MustCount("placement_copies"))
}

func (s *PlacementProtocolSuite) TestInactiveObjectCannotEvictHot() {
	s.mustCreateBucket()
	old := s.mustUploadHot("a.txt", "v1")
	s.mustUpload("a.txt", "v2")
	migration := s.migrationID("evict")
	s.mustReserve(migration, old, 1, model.StorageClassHot, model.StorageClassCold)
	for _, status := range []string{"deleted", "uploading"} {
		s.MustExec(`UPDATE objects SET status = $1`, status)
		_, err := s.placements.ReserveCopy(s.Context(), s.migrationID("new"), old, 1, model.StorageClassHot, model.StorageClassCold)
		s.Require().ErrorIs(err, domainerrors.ErrVersionNotCurrent)
		_, err = s.placements.SwitchPlacement(s.Context(), migration, objectSize, md5OfCopy, time.Hour)
		s.Require().ErrorIs(err, domainerrors.ErrVersionNotCurrent)
		p, err := s.placements.GetPlacement(s.Context(), old)
		s.Require().NoError(err)
		s.Require().False(p.ObjectActive)
		s.Require().Equal(int64(1), p.Generation)
		s.Require().Equal(model.StorageClassHot, p.StorageClass)
	}
}

// Run delete while eviction is waiting for the same object row, then reverse
// the order. The second transaction must wait, and every blob stays accounted.
func (s *PlacementProtocolSuite) TestDeleteAndNoncurrentEvictionSerialize() {
	s.mustCreateBucket()
	for _, deleteFirst := range []bool{false, true} {
		key := "switch-first.txt"
		if deleteFirst {
			key = "delete-first.txt"
		}
		old := s.mustUploadHot(key, "v1")
		current := s.mustUpload(key, "v2")
		oldBlob, currentBlob := s.placementBlob(old), s.placementBlob(current)
		migration := s.migrationID(key)
		target := s.mustReserve(migration, old, 1, model.StorageClassHot, model.StorageClassCold)
		hotBefore, coldBefore := s.usage(model.StorageClassHot), s.usage(model.StorageClassCold)

		s.runDeleteEvictionOrder(key, migration, deleteFirst)

		s.requireOccupied(model.StorageClassHot, hotBefore)
		s.requireOccupied(model.StorageClassCold, coldBefore)
		s.Require().Equal(int64(0), s.MustCount("version_placements"))
		s.expireCleanupDelays()
		due, err := s.placements.ListDueCleanups(s.Context(), 100)
		s.Require().NoError(err)
		orphan := s.copyByBlob(due, currentBlob)
		s.Require().Equal(model.CopyRoleOrphan, orphan.Role)
		s.Require().Nil(orphan.VersionID)
		if deleteFirst {
			orphan = s.copyByBlob(due, oldBlob)
			s.Require().Equal(model.CopyRoleOrphan, orphan.Role)
			copies, err := s.placements.ListMigrationCopies(s.Context(), migration)
			s.Require().NoError(err)
			s.Require().Len(copies, 1)
			s.Require().Nil(copies[0].VersionID)
			s.Require().Equal(model.CopyStateReserved, copies[0].State)
			aborted, err := s.placements.AbortCopy(s.Context(), migration, "object deleted")
			s.Require().NoError(err)
			s.Require().Equal(target.BlobID, aborted.BlobID)
		} else {
			orphan = s.copyByBlob(due, target.BlobID)
			s.Require().Equal(model.CopyRoleOrphan, orphan.Role)
			s.Require().Equal(model.StorageClassCold, orphan.StorageClass)
			source := s.copyByBlob(due, oldBlob)
			s.Require().Equal(model.CopyRoleSource, source.Role)
			s.Require().Equal(model.CopyStateCleanupPending, source.State)
			s.Require().Equal(model.StorageClassHot, source.StorageClass)
		}
	}
}

func (s *PlacementProtocolSuite) runDeleteEvictionOrder(key, migration string, deleteFirst bool) {
	s.T().Helper()
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	first, err := s.client.DB().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	s.Require().NoError(err)
	defer first.Rollback(context.Background())
	firstCtx := txctx.InjectTx(ctx, first)
	// Hold only the object row: delete-first must not wait for a target row
	// already locked by the switch that is waiting for this object.
	_, err = s.objects.LockForDelete(firstCtx, s.bucketName(), key)
	s.Require().NoError(err)
	second, err := s.client.DB().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	s.Require().NoError(err)
	defer second.Rollback(context.Background())
	secondCtx := txctx.InjectTx(ctx, second)
	firstPID, secondPID := int32(first.Conn().PgConn().PID()), int32(second.Conn().PgConn().PID())
	done := make(chan struct{})
	var secondErr error
	go func() {
		defer close(done)
		if deleteFirst {
			_, secondErr = s.placements.SwitchPlacement(secondCtx, migration, objectSize, md5OfCopy, time.Hour)
		} else {
			_, _, secondErr = s.objectSvc.DeleteObjectMeta(secondCtx, s.bucketName(), key)
		}
	}()
	// Join before rolling back a connection that may still be querying.
	defer func() { cancel(); <-done }()
	s.Require().Eventually(func() bool {
		var blocked bool
		e := s.Pool().QueryRow(ctx, `SELECT $2::int = ANY(pg_blocking_pids($1::int))`, secondPID, firstPID).Scan(&blocked)
		return e == nil && blocked
	}, 3*time.Second, 10*time.Millisecond)
	if deleteFirst {
		_, _, err = s.objectSvc.DeleteObjectMeta(firstCtx, s.bucketName(), key)
	} else {
		_, err = s.placements.SwitchPlacement(firstCtx, migration, objectSize, md5OfCopy, time.Hour)
	}
	s.Require().NoError(err)
	s.Require().NoError(first.Commit(ctx))
	<-done
	if deleteFirst {
		s.Require().ErrorIs(secondErr, domainerrors.ErrVersionNotCurrent)
		s.Require().NoError(second.Rollback(ctx))
	} else {
		s.Require().NoError(secondErr)
		s.Require().NoError(second.Commit(ctx))
	}
}
