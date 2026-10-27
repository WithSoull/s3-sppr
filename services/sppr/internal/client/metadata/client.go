package metadata

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
)

type placementClient struct {
	api     metadatav1.PlacementServiceClient
	timeout time.Duration
}

func NewClient(api metadatav1.PlacementServiceClient, timeout time.Duration) client.PlacementClient {
	return &placementClient{api: api, timeout: timeout}
}

func (c *placementClient) GetPlacement(ctx context.Context, versionID string) (model.Placement, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.api.GetPlacement(callCtx, &metadatav1.GetPlacementRequest{VersionId: versionID})
	if err != nil {
		return model.Placement{}, client.FromGRPC(ctx, err)
	}
	p := resp.GetPlacement()
	return model.Placement{
		VersionID:  p.GetVersionId(),
		Bucket:     p.GetBucketName(),
		Key:        p.GetKey(),
		IsCurrent:  p.GetIsCurrent(),
		Class:      model.StorageClass(p.GetStorageClass()),
		BlobID:     p.GetBlobId(),
		Generation: p.GetGeneration(),
		SizeBytes:  p.GetSizeBytes(),
		UpdatedAt:  time.UnixMilli(p.GetUpdatedAt()).UTC(),
	}, nil
}

func (c *placementClient) ReserveCopy(ctx context.Context, a model.ActionSpec) (model.PlacementCopy, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.api.ReserveCopy(callCtx, &metadatav1.ReserveCopyRequest{
		MigrationId:        a.ActionID,
		VersionId:          a.VersionID,
		ExpectedGeneration: a.ExpectedGeneration,
		SourceClass:        string(a.SourceClass),
		TargetClass:        string(a.TargetClass),
	})
	if err != nil {
		return model.PlacementCopy{}, client.FromGRPC(ctx, err)
	}
	return copyFromProto(resp.GetCopy()), nil
}

func (c *placementClient) SwitchPlacement(ctx context.Context, req model.SwitchRequest) error {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.api.SwitchPlacement(callCtx, &metadatav1.SwitchPlacementRequest{
		MigrationId:          req.MigrationID,
		VerifiedSizeBytes:    req.VerifiedSizeBytes,
		VerifiedMd5:          req.VerifiedMD5,
		SourceCleanupDelayMs: req.SourceCleanupDelay.Milliseconds(),
	})
	return client.FromGRPC(ctx, err)
}

func (c *placementClient) AbortCopy(ctx context.Context, migrationID, reason string) error {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.api.AbortCopy(callCtx, &metadatav1.AbortCopyRequest{MigrationId: migrationID, Reason: reason})
	return client.FromGRPC(ctx, err)
}

func (c *placementClient) ListMigrationCopies(ctx context.Context, migrationID string) ([]model.PlacementCopy, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.api.ListMigrationCopies(callCtx, &metadatav1.ListMigrationCopiesRequest{MigrationId: migrationID})
	if err != nil {
		return nil, client.FromGRPC(ctx, err)
	}
	copies := make([]model.PlacementCopy, 0, len(resp.GetCopies()))
	for _, c := range resp.GetCopies() {
		copies = append(copies, copyFromProto(c))
	}
	return copies, nil
}

func copyFromProto(c *metadatav1.PlacementCopyInfo) model.PlacementCopy {
	return model.PlacementCopy{
		CopyID:             c.GetCopyId(),
		Role:               model.CopyRole(c.GetRole()),
		Class:              model.StorageClass(c.GetStorageClass()),
		BlobID:             c.GetBlobId(),
		SizeBytes:          c.GetSizeBytes(),
		ExpectedGeneration: c.GetExpectedGeneration(),
		ExpectedClass:      model.StorageClass(c.GetExpectedClass()),
		State:              model.CopyState(c.GetState()),
	}
}
