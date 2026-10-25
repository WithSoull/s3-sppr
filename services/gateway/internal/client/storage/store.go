package storage

import (
	"context"
	"errors"
	"io"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"

	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
)

const uploadChunkSize = 64 * 1024

func (c *client) Store(ctx context.Context, body io.Reader, size *int64, contentType string) (result model.StoredObject, resultErr error) {
	defer func() { resultErr = mapStoreError(resultErr) }()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.grpc.StoreObject(streamCtx)
	if err != nil {
		return model.StoredObject{}, err
	}
	header := &storagev1.StoreObjectHeader{ContentType: contentType, Size: size}
	if err := stream.Send(&storagev1.StoreObjectRequest{Payload: &storagev1.StoreObjectRequest_Header{Header: header}}); err != nil {
		return model.StoredObject{}, storeSendError(stream, err)
	}

	buffer := make([]byte, uploadChunkSize)
	var total int64
	emptyReads := 0
	for {
		n, readErr := body.Read(buffer)
		if n > 0 {
			emptyReads = 0
			if err := stream.Send(&storagev1.StoreObjectRequest{Payload: &storagev1.StoreObjectRequest_Chunk{
				Chunk: &storagev1.StoreObjectChunk{Data: buffer[:n]},
			}}); err != nil {
				return model.StoredObject{}, storeSendError(stream, err)
			}
			total += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return model.StoredObject{}, &domainerrors.BodyReadError{Err: readErr}
		}
		if n == 0 {
			emptyReads++
			if emptyReads >= 100 {
				return model.StoredObject{}, &domainerrors.BodyReadError{Err: io.ErrNoProgress}
			}
		}
	}

	response, err := stream.CloseAndRecv()
	if err != nil {
		return model.StoredObject{}, err
	}
	return model.StoredObject{BlobID: response.GetBlobId(), ETag: response.GetChecksumMd5(), Size: total}, nil
}

func storeSendError(stream storagev1.DataStorageService_StoreObjectClient, sendErr error) error {
	if errors.Is(sendErr, io.EOF) {
		if _, err := stream.CloseAndRecv(); err != nil {
			return err
		}
	}
	return sendErr
}
