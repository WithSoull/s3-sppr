// Command smoke checks HTTP -> Gateway -> Kafka -> SPPR -> PostgreSQL.
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

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	gateway := flag.String("gateway", "http://127.0.0.1:8080", "Gateway HTTP URL; access events must be enabled")
	metadata := flag.String("metadata", "127.0.0.1:50052", "Metadata gRPC address")
	dsn := flag.String("pg-dsn", os.Getenv("SPPR_SMOKE_PG_DSN"), "test SPPR PostgreSQL DSN (or SPPR_SMOKE_PG_DSN)")
	bootstrap := flag.String("kafka", "localhost:9092", "Kafka bootstrap addresses")
	topic := flag.String("topic", "opens3.access.v1", "Gateway/SPPR access topic")
	timeout := flag.Duration("timeout", 2*time.Minute, "whole smoke timeout")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := run(ctx, strings.TrimRight(*gateway, "/"), *metadata, *dsn, *bootstrap, *topic); err != nil {
		fmt.Fprintln(os.Stderr, "SPPR smoke failed:", err)
		os.Exit(1)
	}
}

type expectedEvent struct {
	operation, key, version, result string
	bytes                           int64
}

func run(ctx context.Context, gateway, metadata, dsn, bootstrap, topic string) error {
	if dsn == "" {
		return errors.New("set SPPR_SMOKE_PG_DSN or -pg-dsn to a test database")
	}
	database, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer database.Close(context.Background())
	conn, err := grpc.NewClient(metadata, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	if err := eventually(ctx, func() (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway+"/healthz", nil)
		if err != nil {
			return false, err
		}
		response, err := client.Do(req)
		if err != nil {
			return false, err
		}
		response.Body.Close()
		return response.StatusCode == http.StatusOK, nil
	}); err != nil {
		return fmt.Errorf("Gateway readiness: %w", err)
	}
	bucket := "access-smoke-" + uuid.NewString()
	_, err = metadatav1.NewMetadataServiceClient(conn).CreateBucket(ctx, &metadatav1.CreateBucketRequest{Name: bucket, OwnerId: uuid.NewString()})
	if err != nil {
		return fmt.Errorf("create smoke bucket: %w", err)
	}
	fmt.Println("Smoke bucket:", bucket)
	var expected []expectedEvent
	var readBytes int64
	for _, tc := range []struct {
		key, body string
		chunked   bool
	}{
		{key: "empty"},
		{key: "nested/binary", body: "\x00binary\xffdata\n"},
		{key: "chunked", body: strings.Repeat("chunk", 1701), chunked: true},
	} {
		objectURL := gateway + "/objects/" + bucket + "/" + tc.key
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, objectURL, strings.NewReader(tc.body))
		if err != nil {
			return err
		}
		if tc.chunked {
			req.ContentLength = -1
		}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		response.Body.Close()
		version := response.Header.Get("X-Object-Version-Id")
		if response.StatusCode != http.StatusOK || version == "" {
			return fmt.Errorf("PUT %s: status=%s, version=%q", tc.key, response.Status, version)
		}
		expected = append(expected, expectedEvent{"write", tc.key, version, "success", int64(len(tc.body))})
		get, err := http.NewRequestWithContext(ctx, http.MethodGet, objectURL, nil)
		if err != nil {
			return err
		}
		response, err = client.Do(get)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK || !bytes.Equal(body, []byte(tc.body)) || response.Header.Get("X-Object-Version-Id") != version {
			return fmt.Errorf("GET %s: status=%s, body/metadata mismatch or read error: %v", tc.key, response.Status, readErr)
		}
		expected = append(expected, expectedEvent{"read", tc.key, version, "success", int64(len(body))})
		readBytes += int64(len(body))
	}
	missing, err := http.NewRequestWithContext(ctx, http.MethodGet, gateway+"/objects/"+bucket+"/missing", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(missing)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("missing GET: %s", response.Status)
	}
	expected = append(expected, expectedEvent{"read", "missing", "", "error", 0})
	if err := eventually(ctx, func() (bool, error) {
		var count int
		err := database.QueryRow(ctx, "SELECT count(*) FROM access_events WHERE bucket=$1", bucket).Scan(&count)
		return count >= len(expected), err
	}); err != nil {
		return fmt.Errorf("wait for Gateway events in SPPR: %w", err)
	}
	rows, err := database.Query(ctx, `SELECT operation, object_key, version_id, result, bytes
		FROM access_events WHERE bucket=$1 ORDER BY sequence`, bucket)
	if err != nil {
		return err
	}
	var actual []expectedEvent
	for rows.Next() {
		var e expectedEvent
		if err := rows.Scan(&e.operation, &e.key, &e.version, &e.result, &e.bytes); err != nil {
			rows.Close()
			return err
		}
		actual = append(actual, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("history count: got %d, want %d", len(actual), len(expected))
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return fmt.Errorf("event %d: got %+v, want %+v", i, actual[i], expected[i])
		}
	}
	checkDemand := func() error {
		var reads, size int64
		err := database.QueryRow(ctx, "SELECT COALESCE(sum(reads),0), COALESCE(sum(bytes),0) FROM read_aggregates WHERE bucket=$1", bucket).Scan(&reads, &size)
		if err != nil {
			return err
		}
		if reads != 3 || size != readBytes {
			return fmt.Errorf("demand: reads=%d bytes=%d, want 3/%d", reads, size, readBytes)
		}
		return nil
	}
	if err := checkDemand(); err != nil {
		return err
	}
	if err := replayAndGap(ctx, database, bucket, bootstrap, topic); err != nil {
		return err
	}
	if err := checkDemand(); err != nil {
		return fmt.Errorf("replay/migration changed demand: %w", err)
	}
	fmt.Printf("PASS HTTP outcomes: 3 PUT, 3 GET, 1 missing GET; demand=3 reads/%d bytes\n", readBytes)
	fmt.Println("PASS duplicate replay counts once; migration events preserve demand; gap 2..2 recorded")
	return nil
}

func replayAndGap(ctx context.Context, database *pgx.Conn, bucket, bootstrap, topic string) error {
	e := model.AccessEvent{SchemaVersion: model.EventSchemaVersion}
	var partition int32
	err := database.QueryRow(ctx, `SELECT gateway_id, stream_id, sequence, occurred_at, bucket, object_key,
		version_id, operation, origin, result, bytes, partition FROM access_events
		WHERE bucket=$1 AND operation='read' AND result='success' ORDER BY sequence LIMIT 1`, bucket).
		Scan(&e.GatewayID, &e.StreamID, &e.Sequence, &e.OccurredAt, &e.Bucket, &e.ObjectKey,
			&e.VersionID, &e.Operation, &e.Origin, &e.Result, &e.Bytes, &partition)
	if err != nil {
		return err
	}
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	cfg.Net.DialTimeout = 2 * time.Second
	cfg.Net.ReadTimeout = 2 * time.Second
	cfg.Net.WriteTimeout = 2 * time.Second
	cfg.Producer.Retry.Max = 1
	producer, err := sarama.NewSyncProducer(strings.Split(bootstrap, ","), cfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	send := func() error {
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Partition: partition,
			Key: sarama.StringEncoder(e.StreamKey()), Value: sarama.ByteEncoder(body)})
		return err
	}
	if err := send(); err != nil {
		return fmt.Errorf("replay delivered Gateway event: %w", err)
	}
	e.StreamID = "smoke-gap-" + uuid.NewString()
	e.Origin = "migration"
	e.Sequence = 1
	if err := send(); err != nil {
		return err
	}
	e.Sequence = 3
	e.OccurredAt = e.OccurredAt.Add(time.Second)
	if err := send(); err != nil {
		return err
	}
	return eventually(ctx, func() (bool, error) {
		var count int
		err := database.QueryRow(ctx, `SELECT count(*) FROM ingestion_gaps
			WHERE gateway_id=$1 AND stream_id=$2 AND first_sequence=2 AND last_sequence=2`, e.GatewayID, e.StreamID).Scan(&count)
		return count == 1, err
	})
}

func eventually(ctx context.Context, check func() (bool, error)) error {
	var lastErr error
	for {
		ok, err := check()
		if ok && err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last query/request: %v)", ctx.Err(), lastErr)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
