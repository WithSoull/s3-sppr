package metadata

import (
	"fmt"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metadata throttles unary RPCs with ResourceExhausted; it says nothing about storage capacity.
func mapError(err error) error {
	if status.Code(err) == codes.ResourceExhausted {
		return fmt.Errorf("%w: %w", domainerrors.ErrMetadataOverloaded, err)
	}
	return err
}
