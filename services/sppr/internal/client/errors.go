package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FromGRPC maps a gRPC status to the model sentinel the executor branches on.
func FromGRPC(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	var sentinel error
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded:
		sentinel = model.ErrUpstreamUnavailable
	case codes.NotFound:
		sentinel = model.ErrUpstreamNotFound
	case codes.Aborted:
		sentinel = model.ErrStale
	case codes.ResourceExhausted:
		sentinel = model.ErrNoCapacity
	case codes.FailedPrecondition:
		sentinel = model.ErrUpstreamPrecondition
	case codes.Canceled:
		return context.Canceled
	default:
		return err
	}
	return fmt.Errorf("%w: %s", sentinel, st.Message())
}

func IsTransient(err error) bool { return errors.Is(err, model.ErrUpstreamUnavailable) }
