package object

import (
	"context"
	"encoding/json"

	domainerrors "github.com/alesplll/opens3-rebac/services/metadata/internal/errors/domain_errors"
)

type objectDeletedEvent struct {
	ObjectID string `json:"object_id"`
	BlobID   string `json:"blob_id"`
}

func (s *objectService) DeleteObjectMeta(ctx context.Context, bucketName, key string) (string, string, error) {
	var objectID, blobID string
	txErr := s.txManager.ReadCommitted(ctx, func(ctx context.Context) error {
		lockedID, err := s.repo.LockForDelete(ctx, bucketName, key)
		if err != nil {
			return err
		}

		// Blobs stay in Storage and in class usage until the executor's cleanup
		// deletes them; the delay lets GETs started before the delete finish.
		if err := s.copyRepo.InsertOrphans(ctx, lockedID, s.deleteCleanupDelay); err != nil {
			return err
		}

		objectID, blobID, err = s.repo.Delete(ctx, bucketName, key)
		return err
	})
	if txErr != nil {
		return "", "", txErr
	}

	payload, _ := json.Marshal(objectDeletedEvent{ObjectID: objectID, BlobID: blobID})
	if err := s.objectDeleted.Send(ctx, []byte(objectID), payload, nil); err != nil {
		return "", "", domainerrors.ErrInternal
	}

	return objectID, blobID, nil
}
