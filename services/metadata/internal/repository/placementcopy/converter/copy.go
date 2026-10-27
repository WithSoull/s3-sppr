package converter

import (
	domainModel "github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	repoModel "github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placementcopy/model"
)

func CopyToDomain(c *repoModel.PlacementCopy) *domainModel.PlacementCopy {
	var expectedClass *domainModel.StorageClass
	if c.ExpectedClass != nil {
		class := domainModel.StorageClass(*c.ExpectedClass)
		expectedClass = &class
	}

	var migrationID string
	if c.MigrationID != nil {
		migrationID = *c.MigrationID
	}

	return &domainModel.PlacementCopy{
		CopyID:             c.CopyID,
		MigrationID:        migrationID,
		VersionID:          c.VersionID,
		Role:               domainModel.CopyRole(c.Role),
		StorageClass:       domainModel.StorageClass(c.StorageClass),
		BlobID:             c.BlobID,
		SizeBytes:          c.SizeBytes,
		ExpectedGeneration: c.ExpectedGeneration,
		ExpectedClass:      expectedClass,
		State:              domainModel.CopyState(c.State),
		VerifiedMD5:        c.VerifiedMD5,
		CleanupAfter:       c.CleanupAfter,
		CleanupAttempts:    c.CleanupAttempts,
		LastError:          c.LastError,
		CreatedAt:          c.CreatedAt,
		UpdatedAt:          c.UpdatedAt,
	}
}

func CopiesToDomain(rows []repoModel.PlacementCopy) []*domainModel.PlacementCopy {
	copies := make([]*domainModel.PlacementCopy, 0, len(rows))
	for i := range rows {
		copies = append(copies, CopyToDomain(&rows[i]))
	}

	return copies
}
