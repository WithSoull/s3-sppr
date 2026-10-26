package placement

import (
	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

func placementToProto(p *model.Placement) *metadatav1.PlacementInfo {
	return &metadatav1.PlacementInfo{
		VersionId:    p.VersionID,
		ObjectId:     p.ObjectID,
		BucketName:   p.BucketName,
		Key:          p.Key,
		IsCurrent:    p.IsCurrent,
		StorageClass: string(p.StorageClass),
		BlobId:       p.BlobID,
		Generation:   p.Generation,
		SizeBytes:    p.SizeBytes,
		UpdatedAt:    p.UpdatedAt.UnixMilli(),
	}
}

func copyToProto(c *model.PlacementCopy) *metadatav1.PlacementCopyInfo {
	info := &metadatav1.PlacementCopyInfo{
		CopyId:          c.CopyID,
		MigrationId:     c.MigrationID,
		Role:            string(c.Role),
		StorageClass:    string(c.StorageClass),
		BlobId:          c.BlobID,
		SizeBytes:       c.SizeBytes,
		State:           string(c.State),
		VerifiedMd5:     c.VerifiedMD5,
		CleanupAttempts: c.CleanupAttempts,
		LastError:       c.LastError,
		CreatedAt:       c.CreatedAt.UnixMilli(),
		UpdatedAt:       c.UpdatedAt.UnixMilli(),
	}
	if c.VersionID != nil {
		info.VersionId = *c.VersionID
	}
	if c.ExpectedGeneration != nil {
		info.ExpectedGeneration = *c.ExpectedGeneration
	}
	if c.ExpectedClass != nil {
		info.ExpectedClass = string(*c.ExpectedClass)
	}
	if c.CleanupAfter != nil {
		info.CleanupAfter = c.CleanupAfter.UnixMilli()
	}

	return info
}

func copiesToProto(copies []*model.PlacementCopy) []*metadatav1.PlacementCopyInfo {
	res := make([]*metadatav1.PlacementCopyInfo, 0, len(copies))
	for _, c := range copies {
		res = append(res, copyToProto(c))
	}

	return res
}
