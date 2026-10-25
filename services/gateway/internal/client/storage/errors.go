package storage

import (
	"fmt"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mapStoreError(err error) error {
	if status.Code(err) == codes.ResourceExhausted {
		return fmt.Errorf("%w: %w", domainerrors.ErrInsufficientStorage, err)
	}
	return err
}

// Metadata already pointed at this blob, so NotFound means lost data, not a missing object.
func mapRetrieveError(err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %w", domainerrors.ErrBlobMissing, err)
	}
	return err
}
