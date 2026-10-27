package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

const classObjectSize = 1<<20 + 7

// storageClasses keeps Gateway routing and the Metadata protocol independently
// testable without SPPR. Executor orchestration is checked by smoke-migration.
func storageClasses(ctx context.Context, httpClient *http.Client, gateway, bucket string, metaConn, hotConn, coldConn *grpc.ClientConn) error {
	meta := metadatav1.NewMetadataServiceClient(metaConn)
	placements := metadatav1.NewPlacementServiceClient(metaConn)
	hot := storagev1.NewDataStorageServiceClient(hotConn)
	cold := storagev1.NewDataStorageServiceClient(coldConn)
	key := "classes/report.bin"
	url := gateway + "/objects/" + bucket + "/" + key

	v1 := pattern(classObjectSize, 3)
	if err := put(ctx, httpClient, url, v1, false); err != nil {
		return fmt.Errorf("PUT new key: %w", err)
	}
	current, err := meta.GetObjectMeta(ctx, &metadatav1.GetObjectMetaRequest{BucketName: bucket, Key: key})
	if err != nil {
		return err
	}
	if current.GetStorageClass() != "COLD" {
		return fmt.Errorf("new key placed in %s, want COLD", current.GetStorageClass())
	}
	fmt.Printf("PASS new key goes to COLD (generation %d)\n", current.GetPlacementGeneration())

	migrationID := uuid.NewString()
	reserved, err := placements.ReserveCopy(ctx, &metadatav1.ReserveCopyRequest{
		MigrationId:        migrationID,
		VersionId:          current.GetVersionId(),
		ExpectedGeneration: current.GetPlacementGeneration(),
		SourceClass:        "COLD",
		TargetClass:        "HOT",
	})
	if err != nil {
		return fmt.Errorf("reserve HOT copy: %w", err)
	}
	copied, err := copyBlob(ctx, cold, hot, current.GetBlobId(), reserved.GetCopy().GetBlobId(), current.GetSizeBytes())
	if err != nil {
		return fmt.Errorf("copy COLD -> HOT: %w", err)
	}
	if _, err := placements.SwitchPlacement(ctx, &metadatav1.SwitchPlacementRequest{
		MigrationId:          migrationID,
		VerifiedSizeBytes:    current.GetSizeBytes(),
		VerifiedMd5:          copied,
		SourceCleanupDelayMs: (10 * time.Minute).Milliseconds(),
	}); err != nil {
		return fmt.Errorf("switch placement: %w", err)
	}
	fmt.Println("PASS version moved COLD -> HOT through ReserveCopy and SwitchPlacement")

	// The HOT blob ID exists only in the HOT Storage, so this read proves the routing.
	if err := get(ctx, httpClient, url, "", http.StatusOK, v1); err != nil {
		return fmt.Errorf("GET after switch: %w", err)
	}
	fmt.Println("PASS GET after the switch returns the same bytes from HOT")

	if err := get(ctx, httpClient, url, "bytes=1048570-1048580", http.StatusPartialContent, v1[1048570:1048581]); err != nil {
		return fmt.Errorf("range GET: %w", err)
	}
	if err := get(ctx, httpClient, url, "bytes=-5", http.StatusPartialContent, v1[len(v1)-5:]); err != nil {
		return fmt.Errorf("suffix range GET: %w", err)
	}
	if err := get(ctx, httpClient, url, fmt.Sprintf("bytes=%d-", len(v1)), http.StatusRequestedRangeNotSatisfiable, nil); err != nil {
		return fmt.Errorf("unsatisfiable range: %w", err)
	}
	fmt.Println("PASS Range: 206 across a Storage chunk boundary, suffix, 416 past the end")

	v2 := pattern(classObjectSize, 5)
	if err := put(ctx, httpClient, url, v2, false); err != nil {
		return fmt.Errorf("overwrite with size: %w", err)
	}
	if err := expectClass(ctx, meta, bucket, key, "HOT"); err != nil {
		return fmt.Errorf("overwrite with size: %w", err)
	}
	if err := get(ctx, httpClient, url, "", http.StatusOK, v2); err != nil {
		return fmt.Errorf("GET overwrite: %w", err)
	}
	fmt.Println("PASS overwrite of a HOT key with known size stays in HOT")

	v3 := pattern(4096, 9)
	if err := put(ctx, httpClient, url, v3, true); err != nil {
		return fmt.Errorf("chunked overwrite: %w", err)
	}
	if err := expectClass(ctx, meta, bucket, key, "COLD"); err != nil {
		return fmt.Errorf("chunked overwrite: %w", err)
	}
	if err := get(ctx, httpClient, url, "", http.StatusOK, v3); err != nil {
		return fmt.Errorf("GET chunked overwrite: %w", err)
	}
	fmt.Println("PASS overwrite without Content-Length goes to COLD")

	if err := do(ctx, httpClient, http.MethodDelete, url, http.StatusNoContent); err != nil {
		return fmt.Errorf("DELETE: %w", err)
	}
	if err := get(ctx, httpClient, url, "", http.StatusNotFound, nil); err != nil {
		return fmt.Errorf("GET after DELETE: %w", err)
	}
	fmt.Println("PASS DELETE removes the object from the catalog; blobs stay in Storage")
	return nil
}

// copyBlob streams the source blob into the target Storage under targetBlobID and
// returns the MD5 the target computed, after checking it against the source bytes.
func copyBlob(ctx context.Context, source, target storagev1.DataStorageServiceClient, sourceBlobID, targetBlobID string, size int64) (string, error) {
	read, err := source.RetrieveObject(ctx, &storagev1.RetrieveObjectRequest{BlobId: sourceBlobID})
	if err != nil {
		return "", err
	}
	write, err := target.StoreObject(ctx)
	if err != nil {
		return "", err
	}
	if err := write.Send(&storagev1.StoreObjectRequest{Payload: &storagev1.StoreObjectRequest_Header{
		Header: &storagev1.StoreObjectHeader{Size: &size, ContentType: "application/octet-stream", BlobId: targetBlobID},
	}}); err != nil {
		return "", err
	}
	sourceMD5 := md5.New()
	for {
		chunk, err := read.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		sourceMD5.Write(chunk.GetData())
		if err := write.Send(&storagev1.StoreObjectRequest{Payload: &storagev1.StoreObjectRequest_Chunk{
			Chunk: &storagev1.StoreObjectChunk{Data: chunk.GetData()},
		}}); err != nil {
			return "", err
		}
	}
	stored, err := write.CloseAndRecv()
	if err != nil {
		return "", err
	}
	if stored.GetBlobId() != targetBlobID {
		return "", fmt.Errorf("target stored blob %s, want %s", stored.GetBlobId(), targetBlobID)
	}
	if want := fmt.Sprintf("%x", sourceMD5.Sum(nil)); stored.GetChecksumMd5() != want {
		return "", fmt.Errorf("copy MD5 %s differs from source %s", stored.GetChecksumMd5(), want)
	}
	return stored.GetChecksumMd5(), nil
}

func expectClass(ctx context.Context, meta metadatav1.MetadataServiceClient, bucket, key, want string) error {
	current, err := meta.GetObjectMeta(ctx, &metadatav1.GetObjectMetaRequest{BucketName: bucket, Key: key})
	if err != nil {
		return err
	}
	if current.GetStorageClass() != want {
		return fmt.Errorf("version placed in %s, want %s", current.GetStorageClass(), want)
	}
	return nil
}

func pattern(size int, seed byte) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i%251) + seed
	}
	return data
}

func put(ctx context.Context, client *http.Client, url string, data []byte, chunked bool) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	if chunked {
		req.Body = io.NopCloser(bytes.NewReader(data))
		req.ContentLength = -1
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s", response.Status)
	}
	return nil
}

func get(ctx context.Context, client *http.Client, url, byteRange string, wantStatus int, want []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		return fmt.Errorf("status %s, want %d", response.Status, wantStatus)
	}
	if want == nil {
		return nil
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if !bytes.Equal(body, want) {
		return fmt.Errorf("body differs: got %d bytes, want %d", len(body), len(want))
	}
	return nil
}

func do(ctx context.Context, client *http.Client, method, url string, wantStatus int) error {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != wantStatus {
		return fmt.Errorf("status %s, want %d", response.Status, wantStatus)
	}
	return nil
}
