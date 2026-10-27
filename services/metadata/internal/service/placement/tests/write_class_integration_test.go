//go:build integration

package tests

import (
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
)

func (s *PlacementProtocolSuite) TestOverwriteInheritsHotWhileItFits() {
	s.mustCreateBucket()
	size := int64(objectSize)

	decision, err := s.objectSvc.ChooseWriteClass(s.Context(), s.bucketName(), "report.txt", &size)
	s.Require().NoError(err)
	s.Require().Equal(model.WriteClassReasonNewKey, decision.Reason)

	v1 := s.mustUpload("report.txt", "v1")
	decision, err = s.objectSvc.ChooseWriteClass(s.Context(), s.bucketName(), "report.txt", &size)
	s.Require().NoError(err)
	s.Require().Equal(&model.WriteClassDecision{StorageClass: model.StorageClassCold, Reason: model.WriteClassReasonPreviousCold}, decision)

	s.mustReserve(s.migrationID("to-hot"), v1, 1, model.StorageClassCold, model.StorageClassHot)
	_, err = s.placements.SwitchPlacement(s.Context(), s.migrationID("to-hot"), objectSize, md5OfCopy, time.Hour)
	s.Require().NoError(err)

	// HOT holds v1 and fits exactly one more version of the same size.
	s.setCapacity("HOT", 2*objectSize)
	decision, err = s.objectSvc.ChooseWriteClass(s.Context(), s.bucketName(), "report.txt", &size)
	s.Require().NoError(err)
	s.Require().Equal(&model.WriteClassDecision{StorageClass: model.StorageClassHot, Reason: model.WriteClassReasonInherited}, decision)

	decision, err = s.objectSvc.ChooseWriteClass(s.Context(), s.bucketName(), "report.txt", nil)
	s.Require().NoError(err)
	s.Require().Equal(model.WriteClassReasonSizeUnknown, decision.Reason)

	_, v2, _, err := s.objectSvc.CreateObjectVersion(s.Context(), s.bucketName(), "report.txt",
		s.FixtureUUID(s.T().Name(), "blob", "v2"), objectSize, `"etag"`, "text/plain", model.StorageClassHot)
	s.Require().NoError(err)

	current, err := s.objects.GetMeta(s.Context(), s.bucketName(), "report.txt", "")
	s.Require().NoError(err)
	s.Require().Equal(v2, current.VersionID)
	s.Require().Equal(model.StorageClassHot, current.StorageClass)

	// The non-current v1 still occupies HOT until the Planner evicts it.
	classes, err := s.placements.ListStorageClasses(s.Context())
	s.Require().NoError(err)
	var hot *model.StorageClassUsage
	for _, class := range classes {
		if class.Name == model.StorageClassHot {
			hot = class
		}
	}
	s.Require().NotNil(hot)
	s.Require().Equal(int64(2), hot.PlacedVersions)
	s.Require().Equal(int64(0), hot.FreeBytes())

	decision, err = s.objectSvc.ChooseWriteClass(s.Context(), s.bucketName(), "report.txt", &size)
	s.Require().NoError(err)
	s.Require().Equal(&model.WriteClassDecision{StorageClass: model.StorageClassCold, Reason: model.WriteClassReasonHotFull}, decision)
}
