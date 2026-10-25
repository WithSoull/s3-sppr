// Command smoke checks HTTP -> Gateway -> Kafka -> SPPR -> PostgreSQL.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	gateway := flag.String("gateway", "http://127.0.0.1:8080", "Gateway HTTP URL; access events must be enabled")
	metadata := flag.String("metadata", "127.0.0.1:50052", "Metadata gRPC address")
	dsn := flag.String("pg-dsn", os.Getenv("SPPR_SMOKE_PG_DSN"), "test SPPR PostgreSQL DSN (or SPPR_SMOKE_PG_DSN)")
	bootstrap := flag.String("kafka", "localhost:9092", "Kafka bootstrap addresses")
	topic := flag.String("object-read-topic", "object-read", "Gateway/SPPR object-read topic")
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
	key, version string
	bytes        int64
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
	// Each topic has its own stream, so reads and writes are ordered separately.
	var writes, reads []expectedEvent
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
		writes = append(writes, expectedEvent{tc.key, version, int64(len(tc.body))})
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
		reads = append(reads, expectedEvent{tc.key, version, int64(len(body))})
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
	if err := eventually(ctx, func() (bool, error) {
		var count int
		err := database.QueryRow(ctx, "SELECT count(*) FROM access_events WHERE bucket=$1", bucket).Scan(&count)
		return count >= len(writes)+len(reads), err
	}); err != nil {
		return fmt.Errorf("wait for Gateway events in SPPR: %w", err)
	}
	for operation, expected := range map[string][]expectedEvent{"write": writes, "read": reads} {
		if err := checkHistory(ctx, database, bucket, operation, expected); err != nil {
			return err
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
	fmt.Printf("PASS HTTP: 3 PUT, 3 GET recorded; missing GET published nothing; demand=3 reads/%d bytes\n", readBytes)
	fmt.Println("PASS duplicate replay counts once; migration events preserve demand; gap 2..2 recorded")
	return nil
}

// The missing GET must not add an event, so the history must match exactly.
func checkHistory(ctx context.Context, database *pgx.Conn, bucket, operation string, expected []expectedEvent) error {
	rows, err := database.Query(ctx, `SELECT object_key, version_id, bytes FROM access_events
		WHERE bucket=$1 AND operation=$2 AND result='success' ORDER BY sequence`, bucket, operation)
	if err != nil {
		return err
	}
	defer rows.Close()
	var actual []expectedEvent
	for rows.Next() {
		var e expectedEvent
		if err := rows.Scan(&e.key, &e.version, &e.bytes); err != nil {
			return err
		}
		actual = append(actual, e)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("%s history count: got %d, want %d", operation, len(actual), len(expected))
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return fmt.Errorf("%s event %d: got %+v, want %+v", operation, i, actual[i], expected[i])
		}
	}
	return nil
}

// replayAndGap resends a delivered Gateway message byte for byte, then a
// synthetic migration stream 1, 3 into the same partition. Once the gap is
// recorded, the earlier replay has been processed too.
func replayAndGap(ctx context.Context, database *pgx.Conn, bucket, bootstrap, topic string) error {
	var gatewayID, versionID, objectKey string
	var partition int32
	var offset int64
	var occurredAt time.Time
	err := database.QueryRow(ctx, `SELECT gateway_id, object_key, version_id, occurred_at, partition, kafka_offset
		FROM access_events WHERE bucket=$1 AND operation='read' AND topic=$2 ORDER BY sequence LIMIT 1`, bucket, topic).
		Scan(&gatewayID, &objectKey, &versionID, &occurredAt, &partition, &offset)
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
	brokers := strings.Split(bootstrap, ",")
	original, err := fetch(ctx, brokers, cfg, topic, partition, offset)
	if err != nil {
		return fmt.Errorf("read delivered Gateway event: %w", err)
	}
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return err
	}
	defer producer.Close()
	send := func(key, value []byte) error {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Partition: partition,
			Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)})
		return err
	}
	if err := send(original.Key, original.Value); err != nil {
		return fmt.Errorf("replay delivered Gateway event: %w", err)
	}
	streamID := "smoke-gap-" + uuid.NewString()
	for _, sequence := range []int64{1, 3} {
		value, err := proto.Marshal(&events_v1.ObjectRead{
			Position:   &events_v1.StreamPosition{GatewayId: gatewayID, StreamId: streamID, Sequence: sequence},
			OccurredAt: timestamppb.New(occurredAt.Add(time.Duration(sequence) * time.Second)),
			Bucket:     bucket,
			ObjectKey:  objectKey,
			VersionId:  versionID,
			Bytes:      1,
			Origin:     events_v1.Origin_ORIGIN_MIGRATION,
		})
		if err != nil {
			return err
		}
		if err := send([]byte(gatewayID+"/"+streamID), value); err != nil {
			return err
		}
	}
	return eventually(ctx, func() (bool, error) {
		var count int
		err := database.QueryRow(ctx, `SELECT count(*) FROM ingestion_gaps
			WHERE gateway_id=$1 AND stream_id=$2 AND first_sequence=2 AND last_sequence=2`, gatewayID, streamID).Scan(&count)
		return count == 1, err
	})
}

func fetch(ctx context.Context, brokers []string, cfg *sarama.Config, topic string, partition int32, offset int64) (*sarama.ConsumerMessage, error) {
	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()
	claim, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, err
	}
	defer claim.Close()
	select {
	case msg := <-claim.Messages():
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
