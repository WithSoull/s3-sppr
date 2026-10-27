// Command smoke-migration checks Gateway -> SPPR API -> Metadata/Storage migrations.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	storageClient "github.com/alesplll/opens3-rebac/services/sppr/internal/client/storage"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	gateway := flag.String("gateway", "http://localhost:8080", "Gateway HTTP URL")
	sppr := flag.String("sppr", "http://localhost:8086", "SPPR HTTP URL; migration must be enabled")
	metadata := flag.String("metadata", "localhost:50052", "Metadata gRPC address")
	hot := flag.String("storage-hot", "localhost:50056", "HOT Storage gRPC address")
	cold := flag.String("storage-cold", "localhost:50053", "COLD Storage gRPC address")
	timeout := flag.Duration("timeout", 12*time.Minute, "whole smoke timeout, including source cleanup")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := run(ctx, strings.TrimRight(*gateway, "/"), strings.TrimRight(*sppr, "/"), *metadata, *hot, *cold); err != nil {
		fmt.Fprintln(os.Stderr, "migration smoke failed:", err)
		os.Exit(1)
	}
}

type smoke struct {
	http       *http.Client
	api        string
	placements metadatav1.PlacementServiceClient
}
type settings struct {
	MinIntervalSeconds int64 `json:"min_interval_seconds"`
	Paused             bool  `json:"paused"`
}

func run(ctx context.Context, gateway, sppr, metadata, hot, cold string) (result error) {
	dial := func(address string) (*grpc.ClientConn, error) {
		return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	metaConn, err := dial(metadata)
	if err != nil {
		return err
	}
	defer metaConn.Close()
	hotConn, err := dial(hot)
	if err != nil {
		return err
	}
	defer hotConn.Close()
	coldConn, err := dial(cold)
	if err != nil {
		return err
	}
	defer coldConn.Close()
	s := &smoke{http: &http.Client{Timeout: 10 * time.Second}, api: sppr + "/api/migration", placements: metadatav1.NewPlacementServiceClient(metaConn)}
	defer s.http.CloseIdleConnections()
	if err := wait(ctx, func() (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, sppr+"/readyz", nil)
		if err != nil {
			return false, err
		}
		resp, err := s.http.Do(req)
		if err != nil {
			return false, nil
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK, nil
	}); err != nil {
		return err
	}
	var old settings
	if err := s.json(ctx, http.MethodGet, "/settings", nil, &old); err != nil {
		return err
	}
	if old.Paused {
		return errors.New("migration is paused; resume it before running the smoke")
	}
	if err := s.json(ctx, http.MethodPut, "/settings", map[string]int64{"min_interval_seconds": 0}, nil); err != nil {
		return err
	}
	// Defaults are persisted: changing the process env alone cannot disable Δmin.
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, s.json(restore, http.MethodPut, "/settings", map[string]int64{"min_interval_seconds": old.MinIntervalSeconds}, nil))
	}()
	baseline, err := s.hotUsed(ctx)
	if err != nil {
		return err
	}
	meta := metadatav1.NewMetadataServiceClient(metaConn)
	bucket := "migration-smoke-" + uuid.NewString()
	key := "object.bin"
	if _, err := meta.CreateBucket(ctx, &metadatav1.CreateBucketRequest{Name: bucket, OwnerId: uuid.NewString()}); err != nil {
		return err
	}
	fmt.Println("Smoke bucket:", bucket)
	body := bytes.Repeat([]byte("migration smoke\x00\xff"), 8192)
	objectURL := gateway + "/objects/" + bucket + "/" + key
	if _, err := s.object(ctx, http.MethodPut, objectURL, body, http.StatusOK); err != nil {
		return err
	}
	initial, err := meta.GetObjectMeta(ctx, &metadatav1.GetObjectMetaRequest{BucketName: bucket, Key: key})
	if err != nil {
		return err
	}
	if initial.GetStorageClass() != "COLD" {
		return fmt.Errorf("new object class %s, want COLD", initial.GetStorageClass())
	}
	version := initial.GetVersionId()
	plan := uuid.NewString()
	var actions []string
	for _, target := range []string{"HOT", "COLD"} {
		p, err := s.placements.GetPlacement(ctx, &metadatav1.GetPlacementRequest{VersionId: version})
		if err != nil {
			return err
		}
		actionID := uuid.NewString()
		actions = append(actions, actionID)
		request := map[string]any{"plan_id": plan, "actions": []any{map[string]any{"action_id": actionID, "version_id": version,
			"expected_generation": p.GetPlacement().GetGeneration(), "source_class": p.GetPlacement().GetStorageClass(), "target_class": target,
			"size_bytes": int64(len(body)), "depends_on": []string{}}}}
		if err := s.json(ctx, http.MethodPost, "/actions", request, nil); err != nil {
			return err
		}
		if err := wait(ctx, func() (bool, error) {
			var state struct {
				Status    string `json:"status"`
				Reason    string `json:"reason"`
				BlockedBy string `json:"blocked_by"`
			}
			if err := s.json(ctx, http.MethodGet, "/actions/"+actionID, nil, &state); err != nil {
				return false, err
			}
			switch state.Status {
			case "succeeded":
				return true, nil
			case "failed", "cancelled":
				return false, fmt.Errorf("action %s: %s/%s", actionID, state.Status, state.Reason)
			}
			return false, nil
		}); err != nil {
			return fmt.Errorf("wait for %s: %w", actionID, err)
		}
		current, err := meta.GetObjectMeta(ctx, &metadatav1.GetObjectMetaRequest{BucketName: bucket, Key: key})
		if err != nil {
			return err
		}
		if current.GetVersionId() != version || current.GetStorageClass() != target || current.GetPlacementGeneration() != p.GetPlacement().GetGeneration()+1 {
			return errors.New("version/class/generation mismatch after migration")
		}
		got, err := s.object(ctx, http.MethodGet, objectURL, nil, http.StatusOK)
		if err != nil {
			return err
		}
		if !bytes.Equal(body, got) {
			return errors.New("GET bytes differ after migration")
		}
		fmt.Printf("PASS %s -> %s: action=%s, same version and bytes\n", p.GetPlacement().GetStorageClass(), target, actionID)
	}
	for _, actionID := range actions {
		var source *metadatav1.PlacementCopyInfo
		if err := wait(ctx, func() (bool, error) {
			resp, err := s.placements.ListMigrationCopies(ctx, &metadatav1.ListMigrationCopiesRequest{MigrationId: actionID})
			if err != nil {
				return false, err
			}
			for _, c := range resp.GetCopies() {
				if c.GetRole() == "source" {
					source = c
					return c.GetState() == "deleted", nil
				}
			}
			return false, nil
		}); err != nil {
			return fmt.Errorf("source cleanup for %s (check cleanup/read-limit env): %w", actionID, err)
		}
		conn := coldConn
		if source.GetStorageClass() == "HOT" {
			conn = hotConn
		}
		reader, err := storageClient.NewClient(storagev1.NewDataStorageServiceClient(conn)).Open(ctx, source.GetBlobId())
		if reader != nil {
			reader.Close()
		}
		if !errors.Is(err, model.ErrUpstreamNotFound) {
			return fmt.Errorf("source blob still readable or unexpected Storage error: %v", err)
		}
		fmt.Printf("PASS %s source deleted: %s\n", source.GetStorageClass(), source.GetBlobId())
	}
	used, err := s.hotUsed(ctx)
	if err != nil {
		return err
	}
	if used != baseline {
		return fmt.Errorf("HOT occupancy %d, baseline %d; use a dedicated stand without concurrent writes/cleanup", used, baseline)
	}
	fmt.Printf("PASS HOT occupancy returned to %d bytes; Δmin restored on exit\n", baseline)
	return nil
}
func (s *smoke) hotUsed(ctx context.Context) (int64, error) {
	resp, err := s.placements.ListStorageClasses(ctx, &metadatav1.ListStorageClassesRequest{})
	if err != nil {
		return 0, err
	}
	for _, c := range resp.GetStorageClasses() {
		if c.GetName() == "HOT" {
			return c.GetPlacedBytes() + c.GetCopyBytes(), nil
		}
	}
	return 0, errors.New("HOT class missing")
}
func (s *smoke) json(ctx context.Context, method, path string, input, output any) error {
	var data []byte
	var err error
	if input != nil {
		data, err = json.Marshal(input)
		if err != nil {
			return err
		}
	}
	body, err := s.object(ctx, method, s.api+path, data, 0)
	if err != nil {
		return err
	}
	if output != nil {
		return json.Unmarshal(body, output)
	}
	return nil
}
func (s *smoke) object(ctx context.Context, method, url string, body []byte, wantStatus int) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(url, s.api) {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if (wantStatus != 0 && resp.StatusCode != wantStatus) || (wantStatus == 0 && (resp.StatusCode < 200 || resp.StatusCode >= 300)) {
		return nil, fmt.Errorf("%s %s: %s: %s", method, url, resp.Status, string(data))
	}
	return data, nil
}
func wait(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		done, err := check()
		if done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
