package storage

import (
	contracts "github.com/alesplll/opens3-rebac/services/gateway/internal/client"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
)

type client struct {
	grpc                   storagev1.DataStorageServiceClient
	retrieveChunkSizeBytes int
}

func NewClient(grpcClient storagev1.DataStorageServiceClient, retrieveChunkSizeBytes int) contracts.StorageClient {
	return &client{grpc: grpcClient, retrieveChunkSizeBytes: retrieveChunkSizeBytes}
}
