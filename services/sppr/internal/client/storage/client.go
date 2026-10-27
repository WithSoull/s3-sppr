package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"google.golang.org/grpc"
)

const (
	uploadChunkSize = 1 << 20
	// Storage sends chunks of up to 64 MiB (STORAGE_RETRIEVE_CHUNK_SIZE_BYTES).
	maxRecvMessageSize = 64<<20 + 64<<10
)

type storageClient struct {
	api storagev1.DataStorageServiceClient
}

func NewClient(api storagev1.DataStorageServiceClient) client.StorageClient {
	return &storageClient{api: api}
}

func (c *storageClient) Open(ctx context.Context, blobID string) (io.ReadCloser, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.api.RetrieveObject(streamCtx, &storagev1.RetrieveObjectRequest{BlobId: blobID}, grpc.MaxCallRecvMsgSize(maxRecvMessageSize))
	if err != nil {
		cancel()
		return nil, client.FromGRPC(ctx, err)
	}
	// NOT_FOUND arrives with the first message, so read it before returning.
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return &streamReader{ctx: ctx, cancel: cancel, done: true}, nil
	}
	if err != nil {
		cancel()
		return nil, client.FromGRPC(ctx, err)
	}
	return &streamReader{ctx: ctx, stream: stream, buffer: bytes.NewReader(first.GetData()), cancel: cancel}, nil
}

type streamReader struct {
	ctx    context.Context
	stream storagev1.DataStorageService_RetrieveObjectClient
	buffer *bytes.Reader
	cancel context.CancelFunc
	done   bool
}

func (r *streamReader) Read(p []byte) (int, error) {
	for {
		if r.buffer != nil && r.buffer.Len() > 0 {
			return r.buffer.Read(p)
		}
		if r.done {
			return 0, io.EOF
		}
		chunk, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			r.done = true
			return 0, io.EOF
		}
		if err != nil {
			return 0, client.FromGRPC(r.ctx, err)
		}
		r.buffer = bytes.NewReader(chunk.GetData())
	}
}

func (r *streamReader) Close() error {
	r.cancel()
	return nil
}

func (c *storageClient) Store(ctx context.Context, blobID string, size int64, body io.Reader) (string, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.api.StoreObject(streamCtx)
	if err != nil {
		return "", client.FromGRPC(ctx, err)
	}
	header := &storagev1.StoreObjectHeader{Size: &size, ContentType: "application/octet-stream", BlobId: blobID}
	if err := stream.Send(&storagev1.StoreObjectRequest{Payload: &storagev1.StoreObjectRequest_Header{Header: header}}); err != nil {
		return "", sendError(ctx, stream, err)
	}
	buffer := make([]byte, uploadChunkSize)
	for {
		n, readErr := io.ReadFull(body, buffer)
		if n > 0 {
			if err := stream.Send(&storagev1.StoreObjectRequest{Payload: &storagev1.StoreObjectRequest_Chunk{
				Chunk: &storagev1.StoreObjectChunk{Data: buffer[:n]},
			}}); err != nil {
				return "", sendError(ctx, stream, err)
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return "", fmt.Errorf("read source: %w", readErr)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", client.FromGRPC(ctx, err)
	}
	if resp.GetBlobId() != blobID {
		return "", fmt.Errorf("storage stored blob %s, want %s", resp.GetBlobId(), blobID)
	}
	return resp.GetChecksumMd5(), nil
}

// A failed Send hides the server status until CloseAndRecv.
func sendError(ctx context.Context, stream storagev1.DataStorageService_StoreObjectClient, sendErr error) error {
	if errors.Is(sendErr, io.EOF) {
		if _, err := stream.CloseAndRecv(); err != nil {
			return client.FromGRPC(ctx, err)
		}
	}
	return client.FromGRPC(ctx, sendErr)
}

func (c *storageClient) Delete(ctx context.Context, blobID string) error {
	resp, err := c.api.DeleteObject(ctx, &storagev1.DeleteObjectRequest{BlobId: blobID})
	if err != nil {
		return client.FromGRPC(ctx, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("Storage did not confirm deletion of %s", blobID)
	}
	return nil
}
