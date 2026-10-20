package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"

	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

const storageMessageOverheadSize = 64 * 1024

func (c *client) Retrieve(ctx context.Context, blobID string, expectedSize int64) (io.ReadCloser, error) {
	ctx, span := otel.Tracer("gateway.storage").Start(ctx, "storage.RetrieveObject", trace.WithSpanKind(trace.SpanKindClient))
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.grpc.RetrieveObject(
		withTraceContext(streamCtx),
		&storagev1.RetrieveObjectRequest{BlobId: blobID},
		grpc.MaxCallRecvMsgSize(c.retrieveChunkSizeBytes+storageMessageOverheadSize),
	)
	if err != nil {
		cancel()
		err = mapRetrieveError(err)
		finishSpan(span, err)
		return nil, err
	}
	first, err := stream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		cancel()
		err = mapRetrieveError(err)
		finishSpan(span, err)
		return nil, err
	}
	if errors.Is(err, io.EOF) {
		if expectedSize != 0 {
			cancel()
			finishSpan(span, domainerrors.ErrEmptyStorageStream)
			return nil, domainerrors.ErrEmptyStorageStream
		}
		finishSpan(span, nil)
		return &streamReader{cancel: cancel}, nil
	}
	return &streamReader{
		stream: stream,
		buffer: bytes.NewReader(first.GetData()),
		cancel: cancel,
		span:   span,
	}, nil
}

type streamReader struct {
	stream storagev1.DataStorageService_RetrieveObjectClient
	buffer *bytes.Reader
	cancel context.CancelFunc
	span   trace.Span
	end    sync.Once
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
			r.finish(err)
			return 0, err
		}
		r.buffer = bytes.NewReader(chunk.GetData())
	}
}

func (r *streamReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	r.finish(nil)
	return nil
}

func (r *streamReader) finish(err error) {
	r.end.Do(func() { finishSpan(r.span, err) })
}
