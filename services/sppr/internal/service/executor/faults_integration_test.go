//go:build integration

package executor_test

import (
	"context"
	"io"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type placementFault struct {
	client.PlacementClient
	reserve         func(context.Context, model.ActionSpec) (model.PlacementCopy, error)
	switchPlacement func(context.Context, model.SwitchRequest) error
	list            func(context.Context, string) ([]model.PlacementCopy, error)
	due             func(context.Context, int32) ([]model.PlacementCopy, error)
}

func (f *placementFault) ReserveCopy(ctx context.Context, a model.ActionSpec) (model.PlacementCopy, error) {
	if f.reserve != nil {
		return f.reserve(ctx, a)
	}
	return f.PlacementClient.ReserveCopy(ctx, a)
}
func (f *placementFault) SwitchPlacement(ctx context.Context, r model.SwitchRequest) error {
	if f.switchPlacement != nil {
		return f.switchPlacement(ctx, r)
	}
	return f.PlacementClient.SwitchPlacement(ctx, r)
}
func (f *placementFault) ListMigrationCopies(ctx context.Context, id string) ([]model.PlacementCopy, error) {
	if f.list != nil {
		return f.list(ctx, id)
	}
	return f.PlacementClient.ListMigrationCopies(ctx, id)
}
func (f *placementFault) ListDueCleanups(ctx context.Context, limit int32) ([]model.PlacementCopy, error) {
	if f.due != nil {
		return f.due(ctx, limit)
	}
	return f.PlacementClient.ListDueCleanups(ctx, limit)
}

type storageFault struct {
	client.StorageClient
	store  func(context.Context, string, int64, io.Reader) (string, error)
	delete func(context.Context, string) error
}

func (f *storageFault) Store(ctx context.Context, id string, size int64, body io.Reader) (string, error) {
	if f.store != nil {
		return f.store(ctx, id, size, body)
	}
	return f.StorageClient.Store(ctx, id, size, body)
}
func (f *storageFault) Delete(ctx context.Context, id string) error {
	if f.delete != nil {
		return f.delete(ctx, id)
	}
	return f.StorageClient.Delete(ctx, id)
}

type corruptReader struct {
	io.Reader
	changed bool
}

func (r *corruptReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 && !r.changed {
		p[0] ^= 1
		r.changed = true
	}
	return n, err
}

type interruptedReader struct {
	io.Reader
	remaining int
	stop      func() error
}

func (r *interruptedReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, r.stop()
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.Reader.Read(p)
	r.remaining -= n
	return n, err
}
