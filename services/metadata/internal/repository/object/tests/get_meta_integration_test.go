//go:build integration

package tests

import (
	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
)

func (s *ObjectRepositorySuite) TestGetMeta_VersionMustBelongToRequestedObject() {
	versionID := s.mustRecreateVersionBelongingToAnotherObject()

	got, err := s.repo.GetMeta(s.Context(), s.bucketName(), s.objectKey(objectRoleActive), versionID)

	s.Require().Nil(got)
	s.Require().ErrorIs(err, domainerrors.ErrObjectNotFound)
	s.Require().Equal(int64(2), s.MustCount("objects"))
	s.Require().Equal(int64(1), s.MustCount("versions"))
}

func (s *ObjectRepositorySuite) TestGetMeta_ReadsBlobFromPlacement() {
	placementBlobID := s.FixtureUUID(s.T().Name(), "blob", "hot-copy")
	s.mustRecreateMigratedObject(placementBlobID)

	for _, versionID := range []string{"", s.versionID(objectRoleActive)} {
		got, err := s.repo.GetMeta(s.Context(), s.bucketName(), s.objectKey(objectRoleActive), versionID)

		s.Require().NoError(err)
		s.Require().Equal(s.versionID(objectRoleActive), got.VersionID)
		s.Require().Equal(placementBlobID, got.BlobID)
		s.Require().Equal(model.StorageClassHot, got.StorageClass)
		s.Require().Equal(int64(3), got.PlacementGeneration)
	}
}

func (s *ObjectRepositorySuite) TestGetMeta_VersionWithoutPlacementIsNotReadable() {
	s.mustRecreateMigratedObject(s.FixtureUUID(s.T().Name(), "blob", "hot-copy"))
	s.MustExec(`DELETE FROM version_placements`)

	got, err := s.repo.GetMeta(s.Context(), s.bucketName(), s.objectKey(objectRoleActive), "")

	s.Require().Nil(got)
	s.Require().ErrorIs(err, domainerrors.ErrObjectNotFound)
}
