//go:build integration

package tests

import (
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
)

func (s *PlacementProtocolSuite) TestCatalogPagesCurrentVersionsAndUsageCountsAllBlobs() {
	s.mustCreateBucket()
	s.mustUpload("a.txt", "v1")
	s.mustUpload("a.txt", "v2")
	b := s.mustUpload("b.txt", "v1")
	s.mustUpload("c.txt", "v1")
	_, _, err := s.objects.Delete(s.Context(), s.bucketName(), "c.txt")
	s.Require().NoError(err)

	s.mustReserve(s.migrationID("b"), b, 1, model.StorageClassCold, model.StorageClassHot)

	var listed []*model.Placement
	token := ""
	for {
		page, next, err := s.placements.ListPlacements(s.Context(), 1, token)
		s.Require().NoError(err)
		listed = append(listed, page...)
		if next == "" {
			break
		}
		token = next
	}

	s.Require().Len(listed, 2)
	keys := map[string]*model.Placement{}
	for _, p := range listed {
		s.Require().True(p.IsCurrent)
		keys[p.Key] = p
	}
	s.Require().Contains(keys, "a.txt")
	s.Require().False(keys["a.txt"].MigrationInProgress)
	s.Require().True(keys["b.txt"].MigrationInProgress)

	classes, err := s.placements.ListStorageClasses(s.Context())
	s.Require().NoError(err)
	s.Require().Len(classes, 2)
	cold, hot := classes[0], classes[1]
	s.Require().Equal(model.StorageClassCold, cold.Name)
	// Both versions of a.txt and b.txt; c.txt was deleted with its placement.
	s.Require().Equal(int64(3), cold.PlacedVersions)
	s.Require().Equal(int64(3*objectSize), cold.PlacedBytes)
	s.Require().Equal(model.StorageClassHot, hot.Name)
	s.Require().Equal(int64(objectSize), hot.CopyBytes)
	s.Require().Equal(int64(1_000_000-objectSize), hot.FreeBytes())

	_, err = s.placements.SwitchPlacement(s.Context(), s.migrationID("b"), objectSize, md5OfCopy, time.Minute)
	s.Require().NoError(err)
	classes, err = s.placements.ListStorageClasses(s.Context())
	s.Require().NoError(err)
	s.Require().Equal(int64(1), classes[1].PlacedVersions)
	s.Require().Equal(int64(0), classes[1].CopyBytes)
	s.Require().Equal(int64(objectSize), classes[0].CopyBytes)
}
