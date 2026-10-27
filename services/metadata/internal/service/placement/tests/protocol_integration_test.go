//go:build integration

package tests

import (
	"errors"
	"sync"
	"time"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
)

const md5OfCopy = "9e107d9d372bb6826bd81d3542a419d6"

func (s *PlacementProtocolSuite) TestColdHotColdKeepsKeyAndVersion() {
	s.mustCreateBucket()
	versionID := s.mustUpload("report.txt", "v1")
	uploaded, err := s.objects.GetMeta(s.Context(), s.bucketName(), "report.txt", "")
	s.Require().NoError(err)
	s.Require().Equal(model.StorageClassCold, uploaded.StorageClass)
	s.Require().Equal(int64(1), uploaded.PlacementGeneration)

	toHot := s.mustReserve(s.migrationID("to-hot"), versionID, 1, model.StorageClassCold, model.StorageClassHot)
	s.Require().Equal(int64(objectSize), s.usage(model.StorageClassHot).CopyBytes)

	switched, err := s.placements.SwitchPlacement(s.Context(), s.migrationID("to-hot"), objectSize, md5OfCopy, time.Hour)
	s.Require().NoError(err)
	s.Require().Equal(int64(2), switched.NewGeneration)
	s.Require().Equal(uploaded.BlobID, switched.SourceCopy.BlobID)

	hot, err := s.objects.GetMeta(s.Context(), s.bucketName(), "report.txt", "")
	s.Require().NoError(err)
	s.Require().Equal(versionID, hot.VersionID)
	s.Require().Equal(model.StorageClassHot, hot.StorageClass)
	s.Require().Equal(toHot.BlobID, hot.BlobID)

	// Reads that started before the switch may still use the COLD blob.
	s.Require().Equal(int64(objectSize), s.usage(model.StorageClassCold).CopyBytes)
	due, err := s.placements.ListDueCleanups(s.Context(), 10)
	s.Require().NoError(err)
	s.Require().Empty(due)

	s.expireCleanupDelays()
	due, err = s.placements.ListDueCleanups(s.Context(), 10)
	s.Require().NoError(err)
	s.Require().Len(due, 1)
	s.Require().Equal(uploaded.BlobID, due[0].BlobID)
	_, err = s.placements.CompleteCleanup(s.Context(), due[0].CopyID, true, "")
	s.Require().NoError(err)
	s.Require().Equal(int64(0), s.usage(model.StorageClassCold).CopyBytes)

	_, err = s.placements.ReserveCopy(s.Context(), s.migrationID("stale"), versionID, 1, model.StorageClassCold, model.StorageClassHot)
	s.Require().ErrorIs(err, domainerrors.ErrPlacementChanged)

	toCold := s.mustReserve(s.migrationID("to-cold"), versionID, 2, model.StorageClassHot, model.StorageClassCold)
	back, err := s.placements.SwitchPlacement(s.Context(), s.migrationID("to-cold"), objectSize, md5OfCopy, time.Hour)
	s.Require().NoError(err)
	s.Require().Equal(int64(3), back.NewGeneration)

	cold, err := s.objects.GetMeta(s.Context(), s.bucketName(), "report.txt", versionID)
	s.Require().NoError(err)
	s.Require().Equal(model.StorageClassCold, cold.StorageClass)
	s.Require().Equal(toCold.BlobID, cold.BlobID)
	s.Require().Equal(int64(3), cold.PlacementGeneration)
}

func (s *PlacementProtocolSuite) TestRetriesDoNotStartSecondMigration() {
	s.mustCreateBucket()
	versionID := s.mustUpload("a.txt", "v1")

	first := s.mustReserve(s.migrationID("m"), versionID, 1, model.StorageClassCold, model.StorageClassHot)
	again := s.mustReserve(s.migrationID("m"), versionID, 1, model.StorageClassCold, model.StorageClassHot)
	s.Require().Equal(first.CopyID, again.CopyID)
	s.Require().Equal(first.BlobID, again.BlobID)

	_, err := s.placements.ReserveCopy(s.Context(), s.migrationID("other"), versionID, 1, model.StorageClassCold, model.StorageClassHot)
	s.Require().ErrorIs(err, domainerrors.ErrCopyAlreadyReserved)

	switched, err := s.placements.SwitchPlacement(s.Context(), s.migrationID("m"), objectSize, md5OfCopy, time.Minute)
	s.Require().NoError(err)
	replayed, err := s.placements.SwitchPlacement(s.Context(), s.migrationID("m"), objectSize, md5OfCopy, time.Minute)
	s.Require().NoError(err)
	s.Require().Equal(switched.NewGeneration, replayed.NewGeneration)
	s.Require().Equal(switched.SourceCopy.CopyID, replayed.SourceCopy.CopyID)

	p, err := s.placements.GetPlacement(s.Context(), versionID)
	s.Require().NoError(err)
	s.Require().Equal(int64(2), p.Generation)

	copies, err := s.placements.ListMigrationCopies(s.Context(), s.migrationID("m"))
	s.Require().NoError(err)
	s.Require().Len(copies, 2)
	s.Require().Equal(model.CopyStatePromoted, copies[0].State)
	s.Require().Equal(md5OfCopy, copies[0].VerifiedMD5)
	s.Require().Equal(model.CopyRoleSource, copies[1].Role)
}

func (s *PlacementProtocolSuite) TestHotCapacityCountsCopiesUntilCleanup() {
	s.setCapacity("HOT", 1500)
	s.mustCreateBucket()
	first := s.mustUpload("a.txt", "v1")
	second := s.mustUpload("b.txt", "v1")

	s.mustReserve(s.migrationID("a"), first, 1, model.StorageClassCold, model.StorageClassHot)
	_, err := s.placements.ReserveCopy(s.Context(), s.migrationID("b"), second, 1, model.StorageClassCold, model.StorageClassHot)
	s.Require().ErrorIs(err, domainerrors.ErrInsufficientCapacity)

	aborted, err := s.placements.AbortCopy(s.Context(), s.migrationID("a"), "copy md5 mismatch")
	s.Require().NoError(err)
	s.Require().Equal(model.CopyStateCleanupPending, aborted.State)

	// The aborted blob may still be on the HOT volume.
	_, err = s.placements.ReserveCopy(s.Context(), s.migrationID("b"), second, 1, model.StorageClassCold, model.StorageClassHot)
	s.Require().ErrorIs(err, domainerrors.ErrInsufficientCapacity)

	failed, err := s.placements.CompleteCleanup(s.Context(), aborted.CopyID, false, "storage unavailable")
	s.Require().NoError(err)
	s.Require().Equal(model.CopyStateCleanupFailed, failed.State)
	s.Require().Equal(int64(objectSize), s.usage(model.StorageClassHot).CopyBytes)
	due, err := s.placements.ListDueCleanups(s.Context(), 10)
	s.Require().NoError(err)
	s.Require().Len(due, 1)

	_, err = s.placements.CompleteCleanup(s.Context(), aborted.CopyID, true, "")
	s.Require().NoError(err)
	s.mustReserve(s.migrationID("b"), second, 1, model.StorageClassCold, model.StorageClassHot)
}

func (s *PlacementProtocolSuite) TestConcurrentReservationsDoNotOverfillHot() {
	const versions = 8
	s.setCapacity("HOT", 2*objectSize)
	s.mustCreateBucket()

	ids := make([]string, versions)
	for i := range ids {
		ids[i] = s.mustUpload(string(rune('a'+i))+".txt", "v1")
	}

	var wg sync.WaitGroup
	errs := make(chan error, versions)
	for i, versionID := range ids {
		wg.Add(1)
		go func(i int, versionID string) {
			defer wg.Done()
			_, err := s.placements.ReserveCopy(s.Context(), s.migrationID(string(rune('a'+i))), versionID, 1, model.StorageClassCold, model.StorageClassHot)
			errs <- err
		}(i, versionID)
	}
	wg.Wait()
	close(errs)

	reserved, rejected := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			reserved++
		case errors.Is(err, domainerrors.ErrInsufficientCapacity):
			rejected++
		default:
			s.Require().NoError(err)
		}
	}
	s.Require().Equal(2, reserved)
	s.Require().Equal(versions-2, rejected)
	s.Require().Equal(int64(2*objectSize), s.usage(model.StorageClassHot).CopyBytes)
}

func (s *PlacementProtocolSuite) TestOverwriteAfterReservationRejectsSwitch() {
	s.mustCreateBucket()
	oldVersion := s.mustUpload("a.txt", "v1")
	s.mustReserve(s.migrationID("m"), oldVersion, 1, model.StorageClassCold, model.StorageClassHot)

	newVersion := s.mustUpload("a.txt", "v2")

	_, err := s.placements.SwitchPlacement(s.Context(), s.migrationID("m"), objectSize, md5OfCopy, time.Minute)
	s.Require().ErrorIs(err, domainerrors.ErrVersionNotCurrent)

	old, err := s.placements.GetPlacement(s.Context(), oldVersion)
	s.Require().NoError(err)
	s.Require().False(old.IsCurrent)
	s.Require().Equal(model.StorageClassCold, old.StorageClass)
	s.Require().Equal(int64(1), old.Generation)

	current, err := s.objects.GetMeta(s.Context(), s.bucketName(), "a.txt", "")
	s.Require().NoError(err)
	s.Require().Equal(newVersion, current.VersionID)
	s.Require().Equal(model.StorageClassCold, current.StorageClass)

	aborted, err := s.placements.AbortCopy(s.Context(), s.migrationID("m"), "version is not current")
	s.Require().NoError(err)
	s.Require().Equal(model.CopyStateCleanupPending, aborted.State)
}

func (s *PlacementProtocolSuite) TestDeletedObjectLeavesCopyForCleanup() {
	s.mustCreateBucket()
	versionID := s.mustUpload("a.txt", "v1")
	target := s.mustReserve(s.migrationID("m"), versionID, 1, model.StorageClassCold, model.StorageClassHot)

	_, _, err := s.objects.Delete(s.Context(), s.bucketName(), "a.txt")
	s.Require().NoError(err)

	_, err = s.placements.SwitchPlacement(s.Context(), s.migrationID("m"), objectSize, md5OfCopy, time.Minute)
	s.Require().ErrorIs(err, domainerrors.ErrVersionNotCurrent)

	aborted, err := s.placements.AbortCopy(s.Context(), s.migrationID("m"), "object deleted")
	s.Require().NoError(err)
	s.Require().Nil(aborted.VersionID)

	due, err := s.placements.ListDueCleanups(s.Context(), 10)
	s.Require().NoError(err)
	s.Require().Len(due, 1)
	s.Require().Equal(target.BlobID, due[0].BlobID)
	s.Require().Equal(model.StorageClassHot, due[0].StorageClass)
}
