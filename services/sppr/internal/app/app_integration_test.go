//go:build integration

package app

import (
	"context"
	"encoding/json"
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
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type traceCollector struct {
	tracepb.UnimplementedTraceServiceServer
	exports atomic.Int64
}

func (c *traceCollector) Export(context.Context, *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	c.exports.Add(1)
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
	traces := &traceCollector{}
	metrics := &metricsCollector{}
	logs := &logsCollector{}
	tracepb.RegisterTraceServiceServer(collector, traces)
	metricspb.RegisterMetricsServiceServer(collector, metrics)
	logspb.RegisterLogsServiceServer(collector, logs)
	go collector.Serve(collectorListener)
	defer collector.Stop()
	for key, value := range map[string]string{
		"PG_DSN": appDSN, "HTTP_ADDRESS": "127.0.0.1:0", "KAFKA_BOOTSTRAP": bootstrap,
		"KAFKA_ACCESS_TOPIC": "sppr-test-" + suffix, "KAFKA_GROUP_ID": "sppr-test-" + suffix,
		"KAFKA_PROCESS_TIMEOUT": "100ms",
		"KAFKA_RETRY_INTERVAL":  "20ms", "OTEL_EXPORTER_OTLP_ENDPOINT": collectorListener.Addr().String(),
		"OTEL_METRICS_PUSH_TIMEOUT": "20ms", "LOGGER_ENABLE_OLTP": "true",
	} {
		t.Setenv(key, value)
	}
	cfg, err := config.Load("../../.env.example")
	require.NoError(t, err)
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_8_0_0
	saramaCfg.Producer.Return.Successes = true
	admin, err := sarama.NewClusterAdmin(strings.Split(bootstrap, ","), saramaCfg)
	require.NoError(t, err)
	defer admin.Close()
	require.NoError(t, admin.CreateTopic(cfg.Kafka.AccessTopic(), &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false))
	defer admin.DeleteTopic(cfg.Kafka.AccessTopic())
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
	event := model.AccessEvent{SchemaVersion: 1, GatewayID: "test-gateway", StreamID: "run", OccurredAt: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC), Bucket: "b", ObjectKey: "k", VersionID: "v", Operation: "read", Origin: "client", Result: "success", Bytes: 100}
	send := func(sequence int64) {
		t.Helper()
		e := event
		e.Sequence = sequence
		e.OccurredAt = e.OccurredAt.Add(time.Duration(sequence) * time.Second)
		body, err := json.Marshal(e)
		require.NoError(t, err)
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{Topic: cfg.Kafka.AccessTopic(), Key: sarama.StringEncoder(e.StreamKey()), Value: sarama.ByteEncoder(body)})
		require.NoError(t, err)
	}
	reads := func() int64 {
		var n int64
		_ = conn.QueryRow(ctx, "SELECT COALESCE(sum(reads),0) FROM read_aggregates").Scan(&n)
		return n
	}
	send(1)
	send(1)
	send(3)
	require.Eventually(t, func() bool { return reads() == 2 }, 10*time.Second, 20*time.Millisecond)
	var missing int64
	require.NoError(t, conn.QueryRow(ctx, "SELECT missing_events FROM ingestion_streams").Scan(&missing))
	require.Equal(t, int64(1), missing)
	_, err = conn.Exec(ctx, "ALTER TABLE read_aggregates ADD CONSTRAINT test_limit CHECK (reads<=2)")
	require.NoError(t, err)
	send(4)
	send(5)
	require.Eventually(t, func() bool { return status("/readyz") == 503 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(2), reads())
	_, err = conn.Exec(ctx, "ALTER TABLE read_aggregates DROP CONSTRAINT test_limit")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return reads() == 4 && status("/readyz") == 200 }, 5*time.Second, 20*time.Millisecond)
	_, poisonOffset, err := producer.SendMessage(&sarama.ProducerMessage{Topic: cfg.Kafka.AccessTopic(), Key: sarama.StringEncoder(event.StreamKey()), Value: sarama.StringEncoder("invalid-json")})
	require.NoError(t, err)
	send(6)
	require.Eventually(t, func() bool { return status("/readyz") == 503 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, 200, status("/healthz"))
	require.Equal(t, int64(4), reads())
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
	require.Greater(t, traces.exports.Load(), int64(0))
	require.Greater(t, metrics.exports.Load(), int64(0))
	require.Greater(t, logs.exports.Load(), int64(0), "shutdown must flush the OTLP logger")
	offsets, err := admin.ListConsumerGroupOffsets(cfg.Kafka.GroupID(), map[string][]int32{cfg.Kafka.AccessTopic(): {0}})
	require.NoError(t, err)
	block := offsets.GetBlock(cfg.Kafka.AccessTopic(), 0)
	require.NotNil(t, block)
	require.Equal(t, poisonOffset, block.Offset, "offset must stop before the failed event and the later valid event")
}
