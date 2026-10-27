package storage

import (
	"bytes"
	"context"
	"errors"
	"io"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"

	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"google.golang.org/grpc"
)

const storageMessageOverheadSize = 64 * 1024

func (c *client) Retrieve(ctx context.Context, blobID string, size int64, part *model.ContentRange) (io.ReadCloser, error) {
	request := &storagev1.RetrieveObjectRequest{BlobId: blobID}
	expected := size
	if part != nil {
		request.Offset, request.Length = part.Offset, part.Length
		expected = part.Length
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.grpc.RetrieveObject(
		streamCtx,
		request,
		grpc.MaxCallRecvMsgSize(c.retrieveChunkSizeBytes+storageMessageOverheadSize),
	)
	if err != nil {
		cancel()
		return nil, mapRetrieveError(err)
	}
	first, err := stream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		cancel()
		return nil, mapRetrieveError(err)
	}
	if errors.Is(err, io.EOF) {
		if expected != 0 {
			cancel()
			return nil, domainerrors.ErrEmptyStorageStream
		}
		return &streamReader{cancel: cancel}, nil
	}
	return &streamReader{
		stream: stream,
		buffer: bytes.NewReader(first.GetData()),
		cancel: cancel,
	}, nil
}

type streamReader struct {
	stream storagev1.DataStorageService_RetrieveObjectClient
	buffer *bytes.Reader
	cancel context.CancelFunc
}

func (r *streamReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if r.buffer != nil && r.buffer.Len() > 0 {
			return r.buffer.Read(p)
		}
		if r.stream == nil {
			return 0, io.EOF
		}
		chunk, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buffer = bytes.NewReader(chunk.GetData())
	}
}

func (r *streamReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}
