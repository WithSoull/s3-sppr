//go:build integration

package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/config"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The tracer still exports to the collector, but the consumer creates no spans.
type traceCollector struct {
	tracepb.UnimplementedTraceServiceServer
}

func (c *traceCollector) Export(context.Context, *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	return &tracepb.ExportTraceServiceResponse{}, nil
}

type metricsCollector struct {
	metricspb.UnimplementedMetricsServiceServer
	exports atomic.Int64
}

type logsCollector struct {
	logspb.UnimplementedLogsServiceServer
	exports atomic.Int64
}

func (c *logsCollector) Export(context.Context, *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	c.exports.Add(1)
	return &logspb.ExportLogsServiceResponse{}, nil
}

func (c *metricsCollector) Export(context.Context, *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	c.exports.Add(1)
	return &metricspb.ExportMetricsServiceResponse{}, nil
}

func TestAppKafkaIngestionAndShutdown(t *testing.T) {
	bootstrap := os.Getenv("SPPR_TEST_KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		t.Skip("set SPPR_TEST_KAFKA_BOOTSTRAP for the full Kafka/PostgreSQL/OTEL check")
	}
	ctx := context.Background()
	dsn := os.Getenv("SPPR_TEST_PG_DSN")
	require.NotEmpty(t, dsn)
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer conn.Close(ctx)
	suffix := fmt.Sprint(time.Now().UnixNano())
	schema := "sppr_app_test_" + suffix
	_, err = conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, err)
	defer conn.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	_, err = conn.Exec(ctx, "SET search_path TO "+pgx.Identifier{schema}.Sanitize())
	require.NoError(t, err)
	sql, err := os.ReadFile("../../migrations/20261021000000_ingestion.up.sql")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, string(sql))
	require.NoError(t, err)
	appDSN := dsn
	if strings.Contains(appDSN, "://") {
		u, err := url.Parse(appDSN)
		require.NoError(t, err)
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		appDSN = u.String()
	} else {
		appDSN += " search_path=" + schema
	}
	collectorListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	collector := grpc.NewServer()
	metrics := &metricsCollector{}
	logs := &logsCollector{}
	tracepb.RegisterTraceServiceServer(collector, &traceCollector{})
	metricspb.RegisterMetricsServiceServer(collector, metrics)
	logspb.RegisterLogsServiceServer(collector, logs)
	go collector.Serve(collectorListener)
	defer collector.Stop()
	for key, value := range map[string]string{
		"PG_DSN": appDSN, "HTTP_ADDRESS": "127.0.0.1:0", "KAFKA_BOOTSTRAP": bootstrap,
		"KAFKA_OBJECT_READ_TOPIC": "sppr-test-read-" + suffix, "KAFKA_OBJECT_WRITTEN_TOPIC": "sppr-test-written-" + suffix,
		"KAFKA_CONSUMER_GROUP_ID": "sppr-test-" + suffix, "KAFKA_RETRY_INTERVAL": "20ms",
		"OTEL_EXPORTER_OTLP_ENDPOINT": collectorListener.Addr().String(),
		"OTEL_METRICS_PUSH_TIMEOUT":   "20ms", "LOGGER_ENABLE_OLTP": "true",
	} {
		t.Setenv(key, value)
	}
	cfg, err := config.Load("../../.env.example")
	require.NoError(t, err)
	readTopic, writtenTopic := cfg.ObjectReadConsumer.Topic(), cfg.ObjectWrittenConsumer.Topic()
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_8_0_0
	saramaCfg.Producer.Return.Successes = true
	admin, err := sarama.NewClusterAdmin(strings.Split(bootstrap, ","), saramaCfg)
	require.NoError(t, err)
	defer admin.Close()
	for _, topic := range []string{readTopic, writtenTopic} {
		require.NoError(t, admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false))
		defer admin.DeleteTopic(topic)
	}
	producer, err := sarama.NewSyncProducer(strings.Split(bootstrap, ","), saramaCfg)
	require.NoError(t, err)
	defer producer.Close()
	a, err := NewApp(ctx, cfg)
	require.NoError(t, err)
	appCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(appCtx) }()
	runStopped := false
	closed := false
	defer func() {
		cancel()
		if !runStopped {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("application cleanup timed out")
			}
		}
		if !closed {
			shutdown, stop := context.WithTimeout(ctx, 5*time.Second)
			defer stop()
			_ = a.Close(shutdown)
		}
	}()
	url := "http://" + a.listener.Addr().String()
	httpClient := &http.Client{Timeout: time.Second}
	status := func(path string) int {
		response, err := httpClient.Get(url + path)
		if err != nil {
			return 0
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	require.Eventually(t, func() bool { return status("/readyz") == 200 }, 15*time.Second, 50*time.Millisecond)
	occurredAt := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	produce := func(topic, key string, value []byte) int64 {
		t.Helper()
		_, offset, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.ByteEncoder(value)})
		require.NoError(t, err)
		return offset
	}
	read := func(sequence int64) int64 {
		t.Helper()
		value, err := proto.Marshal(&events_v1.ObjectRead{
			Position:   &events_v1.StreamPosition{GatewayId: "test-gateway", StreamId: "reads", Sequence: sequence},
			OccurredAt: timestamppb.New(occurredAt.Add(time.Duration(sequence) * time.Second)),
			Bucket:     "b", ObjectKey: "k", VersionId: "v", Bytes: 100, Origin: events_v1.Origin_ORIGIN_CLIENT,
		})
		require.NoError(t, err)
		return produce(readTopic, "test-gateway/reads", value)
	}
	scalar := func(sql string) int64 {
		var n int64
		_ = conn.QueryRow(ctx, sql).Scan(&n)
		return n
	}
	reads := func() int64 { return scalar("SELECT COALESCE(sum(reads),0) FROM read_aggregates") }
	read(1)
	read(1)
	read(3)
	require.Eventually(t, func() bool { return reads() == 2 }, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(1), scalar("SELECT missing_events FROM ingestion_streams WHERE stream_id='reads'"))

	written, err := proto.Marshal(&events_v1.ObjectWritten{
		Position:   &events_v1.StreamPosition{GatewayId: "test-gateway", StreamId: "writes", Sequence: 1},
		OccurredAt: timestamppb.New(occurredAt), Bucket: "b", ObjectKey: "k", VersionId: "v", Bytes: 100,
		Origin: events_v1.Origin_ORIGIN_CLIENT,
	})
	require.NoError(t, err)
	produce(writtenTopic, "test-gateway/writes", written)
	require.Eventually(t, func() bool {
		return scalar("SELECT count(*) FROM access_events WHERE operation='write' AND topic='"+writtenTopic+"'") == 1
	}, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(2), reads(), "writes are history, not demand")

	// A database failure blocks the partition until the event is stored.
	_, err = conn.Exec(ctx, "ALTER TABLE read_aggregates ADD CONSTRAINT test_limit CHECK (reads<=2)")
	require.NoError(t, err)
	read(4)
	read(5)
	// Polled synchronously: require.Never may leave a check running on conn.
	for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		require.Equal(t, int64(3), scalar("SELECT last_sequence FROM ingestion_streams WHERE stream_id='reads'"))
	}
	_, err = conn.Exec(ctx, "ALTER TABLE read_aggregates DROP CONSTRAINT test_limit")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return reads() == 4 }, 5*time.Second, 20*time.Millisecond)

	// A rejected event is committed and does not block the partition.
	produce(readTopic, "test-gateway/reads", []byte{0xff})
	lastOffset := read(6)
	require.Eventually(t, func() bool { return reads() == 5 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 200, status("/healthz"))
	cancel()
	select {
	case err := <-done:
		runStopped = true
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("application did not stop")
	}
	shutdown, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	require.NoError(t, a.Close(shutdown))
	closed = true
	require.Greater(t, metrics.exports.Load(), int64(0))
	require.Greater(t, logs.exports.Load(), int64(0), "shutdown must flush the OTLP logger")
	offsets, err := admin.ListConsumerGroupOffsets(cfg.Sarama.GroupID(), map[string][]int32{readTopic: {0}})
	require.NoError(t, err)
	block := offsets.GetBlock(readTopic, 0)
	require.NotNil(t, block)
	require.Equal(t, lastOffset+1, block.Offset, "the rejected event must not hold back the committed offset")
}
