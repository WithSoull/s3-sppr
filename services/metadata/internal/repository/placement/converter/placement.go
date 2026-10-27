package converter

import (
	domainModel "github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	repoModel "github.com/alesplll/opens3-rebac/services/metadata/internal/repository/placement/model"
)

func PlacementToDomain(p *repoModel.Placement) *domainModel.Placement {
	return &domainModel.Placement{
		VersionID:        p.VersionID,
		ObjectID:         p.ObjectID,
		BucketName:       p.BucketName,
		Key:              p.Key,
		ObjectActive:     p.ObjectActive,
		IsCurrent:        p.IsCurrent,
		StorageClass:     domainModel.StorageClass(p.StorageClass),
		BlobID:           p.BlobID,
		Generation:       p.Generation,
		SizeBytes:        p.SizeBytes,
		UpdatedAt:        p.UpdatedAt,
		VersionCreatedAt: p.VersionCreatedAt,

		MigrationInProgress: p.MigrationInProgress,
	}
}

func UsageToDomain(u *repoModel.StorageClassUsage) *domainModel.StorageClassUsage {
	return &domainModel.StorageClassUsage{
		Name:           domainModel.StorageClass(u.Name),
		CapacityBytes:  u.CapacityBytes,
		MaxUtilization: u.MaxUtilization,
		UsableBytes:    u.UsableBytes,
		PlacedBytes:    u.PlacedBytes,
		PlacedVersions: u.PlacedVersions,
		CopyBytes:      u.CopyBytes,
		Copies:         u.Copies,
		FailedCleanups: u.FailedCleanups,
	}
}
