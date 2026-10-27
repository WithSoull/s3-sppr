//go:build integration

package tests

import (
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

const objectSize = 1000

func (s *PlacementProtocolSuite) setCapacity(class string, capacityBytes int64) {
	s.T().Helper()

	s.MustExec(`
		UPDATE storage_classes
		SET capacity_bytes = $2, max_utilization = 1
		WHERE name = $1
	`, class, capacityBytes)
}

func (s *PlacementProtocolSuite) bucketName() string {
	return "placement-bucket"
}

func (s *PlacementProtocolSuite) mustCreateBucket() {
	s.T().Helper()

	s.MustExec(`
		INSERT INTO buckets (id, name, owner_id)
		VALUES ($1, $2, $3)
	`, s.FixtureUUID(s.T().Name(), "bucket"), s.bucketName(), s.FixtureUUID(s.T().Name(), "owner"))
}

// mustUpload registers a version the way Gateway does after Storage wrote it.
func (s *PlacementProtocolSuite) mustUpload(key, role string) string {
	s.T().Helper()

	_, versionID, _, err := s.objectSvc.CreateObjectVersion(
		s.Context(),
		s.bucketName(),
		key,
		s.FixtureUUID(s.T().Name(), "upload-blob", key, role),
		objectSize,
		`"etag"`,
		"text/plain",
		"",
	)
	s.Require().NoError(err)

	return versionID
}

func (s *PlacementProtocolSuite) migrationID(name string) string {
	return s.FixtureUUID(s.T().Name(), "migration", name)
}

func (s *PlacementProtocolSuite) mustReserve(migration, versionID string, generation int64, source, target model.StorageClass) *model.PlacementCopy {
	s.T().Helper()

	c, err := s.placements.ReserveCopy(s.Context(), migration, versionID, generation, source, target)
	s.Require().NoError(err)
	s.Require().Equal(model.CopyStateReserved, c.State)

	return c
}

func (s *PlacementProtocolSuite) usage(class model.StorageClass) *model.StorageClassUsage {
	s.T().Helper()

	var u model.StorageClassUsage
	err := s.client.DB().QueryRowContext(s.Context(), dbQuery("usage", `
		SELECT
		  COALESCE((SELECT SUM(v.size_bytes) FROM version_placements vp JOIN versions v ON v.id = vp.version_id WHERE vp.storage_class = $1), 0),
		  COALESCE((SELECT SUM(size_bytes) FROM placement_copies WHERE storage_class = $1 AND state IN ('reserved', 'cleanup_pending', 'cleanup_failed')), 0)
	`), class).Scan(&u.PlacedBytes, &u.CopyBytes)
	s.Require().NoError(err)

	return &u
}

func (s *PlacementProtocolSuite) placementBlob(versionID string) string {
	s.T().Helper()

	p, err := s.placements.GetPlacement(s.Context(), versionID)
	s.Require().NoError(err)

	return p.BlobID
}

// requireOccupied checks that placed and copy bytes together did not shrink:
// a delete moves space from placements to copies, it does not release it.
func (s *PlacementProtocolSuite) requireOccupied(class model.StorageClass, before *model.StorageClassUsage) {
	s.T().Helper()

	now := s.usage(class)
	s.Require().Equal(before.PlacedBytes+before.CopyBytes, now.PlacedBytes+now.CopyBytes)
}

func (s *PlacementProtocolSuite) copyByBlob(copies []*model.PlacementCopy, blobID string) *model.PlacementCopy {
	s.T().Helper()

	for _, c := range copies {
		if c.BlobID == blobID {
			return c
		}
	}
	s.FailNow("no copy for blob " + blobID)

	return nil
}

// expireCleanupDelays moves cleanup_after into the past instead of sleeping.
func (s *PlacementProtocolSuite) expireCleanupDelays() {
	s.T().Helper()

	s.MustExec(`UPDATE placement_copies SET cleanup_after = now() - interval '1 second' WHERE cleanup_after IS NOT NULL`)
}

func dbQuery(name, sql string) db.Query {
	return db.Query{Name: "placement_test:" + name, QueryRaw: sql}
}

func (s *PlacementProtocolSuite) mustUploadHot(key, role string) string {
	s.T().Helper()
	_, versionID, _, err := s.objectSvc.CreateObjectVersion(
		s.Context(), s.bucketName(), key,
		s.FixtureUUID(s.T().Name(), "upload-blob", key, role),
		objectSize, `"etag"`, "text/plain", model.StorageClassHot,
	)
	s.Require().NoError(err)
	return versionID
}
