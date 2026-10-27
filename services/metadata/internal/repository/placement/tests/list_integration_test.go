//go:build integration

package tests

import (
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
)

func (s *PlacementRepositorySuite) TestListPlannerCatalogue() {
	bucketID := s.FixtureUUID(s.T().Name(), "bucket")
	objectID := s.FixtureUUID(s.T().Name(), "object")
	// UUID order differs from version age: the third version is current, while
	// the first (HOT) and second (COLD) versions are old.
	oldHot := "00000000-0000-4000-8000-000000000001"
	current := "00000000-0000-4000-8000-000000000002"
	oldCold := "00000000-0000-4000-8000-000000000003"
	baseTime := time.UnixMilli(1761480000123).UTC()
	created := map[string]time.Time{
		oldHot: baseTime, oldCold: baseTime.Add(time.Hour), current: baseTime.Add(2 * time.Hour),
	}
	s.MustExec(`INSERT INTO buckets (id, name, owner_id) VALUES ($1, 'catalogue-bucket', $2)`, bucketID, s.FixtureUUID(s.T().Name(), "owner"))
	s.MustExec(`INSERT INTO objects (id, bucket_id, key) VALUES ($1, $2, 'key')`, objectID, bucketID)
	for i, id := range []string{oldHot, oldCold, current} {
		blobID := s.FixtureUUID(s.T().Name(), "blob", id)
		s.MustExec(`
   INSERT INTO versions (id, object_id, blob_id, size_bytes, version_number, state, created_at, committed_at)
   VALUES ($1, $2, $3, 1024, $4, 'committed', $5, $5)
  `, id, objectID, blobID, i+1, created[id])
		class := model.StorageClassCold
		if id == oldHot {
			class = model.StorageClassHot
		}
		s.Require().NoError(s.repo.Create(s.Context(), id, blobID, class))
	}
	s.MustExec(`UPDATE objects SET current_version_id = $2 WHERE id = $1`, objectID, current)

	tests := []struct {
		name string
		args model.ListPlacementsRequest
		want []string
	}{
		{name: "default: current only", args: model.ListPlacementsRequest{PageSize: 10}, want: []string{current}},
		{name: "flag: current and old HOT in UUID order", args: model.ListPlacementsRequest{PageSize: 10, IncludeNoncurrentHot: true}, want: []string{oldHot, current}},
		{name: "first page contains old HOT", args: model.ListPlacementsRequest{PageSize: 1, IncludeNoncurrentHot: true}, want: []string{oldHot}},
		{name: "next page contains current", args: model.ListPlacementsRequest{PageSize: 1, PageToken: oldHot, IncludeNoncurrentHot: true}, want: []string{current}},
		{name: "no rows after current", args: model.ListPlacementsRequest{PageSize: 10, PageToken: current, IncludeNoncurrentHot: true}, want: []string{}},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			rows, err := s.repo.List(s.Context(), tt.args)
			s.Require().NoError(err)
			ids := make([]string, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.VersionID)
				s.Require().Equal(row.VersionID == current, row.IsCurrent)
				s.Require().Equal(created[row.VersionID].UnixMilli(), row.VersionCreatedAt.UnixMilli())
				s.Require().False(row.VersionCreatedAt.Equal(row.UpdatedAt))
			}
			s.Require().Equal(tt.want, ids)
		})
	}

	// The shared SELECT must also populate version creation time for GetPlacement.
	for id, wantTime := range created {
		row, err := s.repo.Get(s.Context(), id)
		s.Require().NoError(err)
		s.Require().Equal(wantTime.UnixMilli(), row.VersionCreatedAt.UnixMilli())
	}

	// A current HOT row must be returned once, alongside the old HOT version.
	s.MustExec(`UPDATE version_placements SET storage_class = 'HOT' WHERE version_id = $1`, current)
	rows, err := s.repo.List(s.Context(), model.ListPlacementsRequest{PageSize: 10, IncludeNoncurrentHot: true})
	s.Require().NoError(err)
	s.Require().Len(rows, 2)
	s.Require().Equal(oldHot, rows[0].VersionID)
	s.Require().Equal(current, rows[1].VersionID)

	for _, status := range []string{"deleted", "uploading"} {
		s.MustExec(`UPDATE objects SET status = $2 WHERE id = $1`, objectID, status)
		for _, include := range []bool{false, true} {
			rows, err := s.repo.List(s.Context(), model.ListPlacementsRequest{PageSize: 10, IncludeNoncurrentHot: include})
			s.Require().NoError(err)
			s.Require().Empty(rows, "status=%s, include_noncurrent_hot=%t", status, include)
		}
	}
	s.MustExec(`DELETE FROM objects WHERE id = $1`, objectID)
	rows, err = s.repo.List(s.Context(), model.ListPlacementsRequest{PageSize: 10, IncludeNoncurrentHot: true})
	s.Require().NoError(err)
	s.Require().Empty(rows)
}
