// smoke-history deliberately stops Kafka and kills/stops Gateway on a dedicated
// Compose stand. It leaves the services running again, including on failure.
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
	"os/exec"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	events "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	metadata "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type stand struct {
	db                                              *pgx.Conn
	client                                          *http.Client
	gateway, sppr, token, composeFile, group, topic string
	brokers                                         []string
	outage                                          time.Duration
}

func main() {
	gateway := flag.String("gateway", "http://127.0.0.1:8080", "Gateway URL")
	sppr := flag.String("sppr", "http://127.0.0.1:8086", "SPPR with Planner enabled")
	md := flag.String("metadata", "127.0.0.1:50052", "Metadata gRPC")
	dsn := flag.String("pg-dsn", os.Getenv("SPPR_SMOKE_PG_DSN"), "SPPR test database DSN")
	brokers := flag.String("kafka", "localhost:9092", "Kafka bootstrap")
	topic := flag.String("topic", "object-read", "read topic")
	group := flag.String("group", "sppr", "SPPR consumer group")
	file := flag.String("compose-file", "docker-compose.yml", "dedicated Compose file; run from repo root")
	outage := flag.Duration("outage", 12*time.Second, "Kafka downtime; must be shorter than INGEST_STREAM_LOST_AFTER")
	timeout := flag.Duration("timeout", 5*time.Minute, "whole smoke deadline")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	err := func() error {
		if *dsn == "" {
			return errors.New("set SPPR_SMOKE_PG_DSN or -pg-dsn")
		}
		db, err := pgx.Connect(ctx, *dsn)
		if err != nil {
			return err
		}
		defer db.Close(context.Background())
		s := stand{db: db, client: &http.Client{Timeout: 5 * time.Second}, gateway: strings.TrimRight(*gateway, "/"), sppr: strings.TrimRight(*sppr, "/"), token: os.Getenv("SPPR_SMOKE_ACCESS_TOKEN"), composeFile: *file, brokers: strings.Split(*brokers, ","), topic: *topic, group: *group, outage: *outage}
		return s.run(ctx, *md)
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "history smoke failed:", err)
		os.Exit(1)
	}
}
func (s *stand) compose(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", s.composeFile}, args...)...)
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %v: %w: %s", args, err, b)
	}
	return nil
}
func wait(ctx context.Context, f func() (bool, error)) error {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	var last error
	for {
		ok, err := f()
		if err == nil && ok {
			return nil
		}
		if err != nil {
			last = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait: %w (last error: %v)", ctx.Err(), last)
		case <-tick.C:
		}
	}
}
func (s *stand) request(ctx context.Context, method, url string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	res, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return b, fmt.Errorf("%s %s: %s: %s", method, url, res.Status, b)
	}
	return b, err
}
func (s *stand) status(ctx context.Context) (struct {
	History model.Coverage        `json:"history"`
	Objects []model.ObjectHistory `json:"objects"`
}, error) {
	var st struct {
		History model.Coverage        `json:"history"`
		Objects []model.ObjectHistory `json:"objects"`
	}
	b, err := s.request(ctx, "GET", s.sppr+"/api/planner/status", nil)
	if err == nil {
		err = json.Unmarshal(b, &st)
	}
	return st, err
}
func (s *stand) watermark(ctx context.Context) (time.Time, error) {
	st, err := s.status(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if !st.History.Known {
		return time.Time{}, errors.New("W unknown")
	}
	return st.History.Watermark, nil
}
func (s *stand) run(ctx context.Context, md string) error {
	if err := wait(ctx, func() (bool, error) {
		_, err := s.request(ctx, "GET", s.gateway+"/healthz", nil)
		return err == nil, err
	}); err != nil {
		return fmt.Errorf("Gateway readiness: %w", err)
	}
	// Status also probes Metadata, avoiding a race with Compose service_started.
	if err := wait(ctx, func() (bool, error) { _, err := s.status(ctx); return err == nil, err }); err != nil {
		return fmt.Errorf("SPPR/Metadata readiness: %w", err)
	}
	conn, err := grpc.NewClient(md, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	bucket := "history-smoke-" + uuid.NewString()
	_, err = metadata.NewMetadataServiceClient(conn).CreateBucket(ctx, &metadata.CreateBucketRequest{Name: bucket, OwnerId: uuid.NewString()})
	if err != nil {
		return err
	}
	object := s.gateway + "/objects/" + bucket + "/read"
	idle := s.gateway + "/objects/" + bucket + "/idle"
	for _, url := range []string{object, idle} {
		if _, err = s.request(ctx, "PUT", url, []byte("history smoke")); err != nil {
			return err
		}
	}
	if _, err = s.request(ctx, "GET", object, nil); err != nil {
		return err
	}
	var stream, gateway string
	var partition int32
	var offset int64
	if err = wait(ctx, func() (bool, error) {
		err := s.db.QueryRow(ctx, `SELECT gateway_id,stream_id,partition,kafka_offset FROM access_events WHERE bucket=$1 AND operation='read' ORDER BY sequence LIMIT 1`, bucket).Scan(&gateway, &stream, &partition, &offset)
		return err == nil, err
	}); err != nil {
		return fmt.Errorf("scenario 1 event: %w", err)
	}
	var w time.Time
	if err = wait(ctx, func() (bool, error) { var err error; w, err = s.watermark(ctx); return err == nil, err }); err != nil {
		return err
	}
	if err = wait(ctx, func() (bool, error) { next, err := s.watermark(ctx); return next.After(w), err }); err != nil {
		return fmt.Errorf("scenario 1 idle W: %w", err)
	}
	// Verify the complete, never-read object before introducing a gap.
	if err = s.objectState(ctx, bucket, "idle", "чтений нет"); err != nil {
		return fmt.Errorf("scenario 6 complete: %w", err)
	}
	fmt.Println("PASS 1: GET persisted; heartbeats advance W without further reads")
	if err = s.replay(ctx, partition, offset); err != nil {
		return fmt.Errorf("scenario 2: %w", err)
	}
	var demand int64
	if err = s.db.QueryRow(ctx, `SELECT COALESCE(sum(reads),0)::bigint FROM read_aggregates WHERE bucket=$1`, bucket).Scan(&demand); err != nil {
		return err
	}
	if demand != 1 {
		return fmt.Errorf("scenario 2 duplicate changed demand: %d", demand)
	}
	fmt.Println("PASS 2: byte-for-byte Kafka replay did not double demand")
	// Restore both services even when a scenario fails halfway through.
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if err := s.compose(restore, "start", "kafka", "gateway"); err != nil {
			fmt.Fprintln(os.Stderr, "restore stand:", err)
		}
	}()
	var before int64
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM ingestion_gaps WHERE gateway_id=$1 AND stream_id=$2`, gateway, stream).Scan(&before); err != nil {
		return err
	}
	if err = s.compose(ctx, "stop", "kafka"); err != nil {
		return err
	}
	end := time.Now().Add(s.outage)
	for time.Now().Before(end) {
		if _, err = s.request(ctx, "GET", object, nil); err != nil {
			return fmt.Errorf("scenario 3 GET while Kafka down: %w", err)
		}
	}
	if err = s.compose(ctx, "start", "kafka"); err != nil {
		return err
	}
	if err = wait(ctx, func() (bool, error) {
		var n int64
		err := s.db.QueryRow(ctx, `SELECT count(*) FROM ingestion_gaps WHERE gateway_id=$1 AND stream_id=$2`, gateway, stream).Scan(&n)
		return n > before, err
	}); err != nil {
		return fmt.Errorf("scenario 3 lost events not exposed: %w", err)
	}
	// A new successful read moves the damaged stream's conservative barrier
	// past the recorded gap; the next heartbeat exposes it to feature quality.
	if _, err = s.request(ctx, "GET", object, nil); err != nil {
		return err
	}
	fmt.Println("PASS 3: GET stayed available with Kafka down; gap exposed on recovery")
	if err = s.objectState(ctx, bucket, "idle", "история неполна"); err != nil {
		return fmt.Errorf("scenario 6 gap: %w", err)
	}
	fmt.Println("PASS 6: status distinguishes no reads from incomplete history")
	if err = s.compose(ctx, "kill", "gateway"); err != nil {
		return err
	}
	if err = wait(ctx, func() (bool, error) {
		var n int64
		err := s.db.QueryRow(ctx, `SELECT count(*) FROM ingestion_streams s JOIN ingestion_gaps g USING(gateway_id,stream_id) WHERE s.gateway_id=$1 AND s.stream_id=$2 AND s.lost_at IS NOT NULL AND g.last_sequence IS NULL`, gateway, stream).Scan(&n)
		return n == 1, err
	}); err != nil {
		return fmt.Errorf("scenario 4 lost stream: %w", err)
	}
	if err = s.compose(ctx, "start", "gateway"); err != nil {
		return err
	}
	if err = wait(ctx, func() (bool, error) { next, err := s.watermark(ctx); return next.After(w), err }); err != nil {
		return fmt.Errorf("scenario 4 W after restart: %w", err)
	}
	var newStream string
	if err = wait(ctx, func() (bool, error) {
		err := s.db.QueryRow(ctx, `SELECT stream_id FROM ingestion_streams WHERE gateway_id=$1 AND topic=$2 AND closed_at IS NULL AND stream_id<>$3 ORDER BY created_at DESC LIMIT 1`, gateway, s.topic, stream).Scan(&newStream)
		return err == nil, err
	}); err != nil {
		return err
	}
	// Wait for a baseline W on the new stream, then prove a second advance.
	w, err = s.watermark(ctx)
	if err != nil {
		return err
	}
	if err = wait(ctx, func() (bool, error) { next, err := s.watermark(ctx); return next.After(w), err }); err != nil {
		return err
	}
	fmt.Println("PASS 4: killed stream recorded as lost with unknown-count gap; W grows after restart")
	var gaps int64
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM ingestion_gaps WHERE gateway_id=$1 AND stream_id=$2`, gateway, newStream).Scan(&gaps); err != nil {
		return err
	}
	if err = s.compose(ctx, "stop", "-t", "40", "gateway"); err != nil {
		return err
	}
	if err = wait(ctx, func() (bool, error) {
		var ok bool
		err := s.db.QueryRow(ctx, `SELECT closed_at IS NOT NULL AND lost_at IS NULL FROM ingestion_streams WHERE gateway_id=$1 AND stream_id=$2`, gateway, newStream).Scan(&ok)
		return ok, err
	}); err != nil {
		return fmt.Errorf("scenario 5 closing: %w", err)
	}
	var after int64
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM ingestion_gaps WHERE gateway_id=$1 AND stream_id=$2`, gateway, newStream).Scan(&after); err != nil {
		return err
	}
	if gaps != after {
		return errors.New("scenario 5 graceful stop added a gap")
	}
	fmt.Println("PASS 5: graceful stop closes stream without adding a gap")
	return nil
}
func (s *stand) objectState(ctx context.Context, bucket, key, state string) error {
	return wait(ctx, func() (bool, error) {
		st, err := s.status(ctx)
		if err != nil {
			return false, err
		}
		for _, o := range st.Objects {
			if o.Bucket == bucket && o.ObjectKey == key {
				return o.State == state, nil
			}
		}
		return false, nil
	})
}
func (s *stand) replay(ctx context.Context, partition int32, offset int64) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Return.Errors = true
	c, err := sarama.NewConsumer(s.brokers, cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	pc, err := c.ConsumePartition(s.topic, partition, offset)
	if err != nil {
		return err
	}
	defer pc.Close()
	var msg *sarama.ConsumerMessage
	select {
	case msg = <-pc.Messages():
	case err := <-pc.Errors():
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
	if msg == nil || msg.Offset != offset {
		return errors.New("original Kafka message unavailable")
	}
	var event events.ObjectRead
	if err = proto.Unmarshal(msg.Value, &event); err != nil {
		return err
	}
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	p, err := sarama.NewSyncProducer(s.brokers, cfg)
	if err != nil {
		return err
	}
	defer p.Close()
	_, replayOffset, err := p.SendMessage(&sarama.ProducerMessage{Topic: s.topic, Partition: partition, Key: sarama.ByteEncoder(msg.Key), Value: sarama.ByteEncoder(msg.Value)})
	if err != nil {
		return err
	}
	admin, err := sarama.NewClusterAdmin(s.brokers, cfg)
	if err != nil {
		return err
	}
	defer admin.Close()
	return wait(ctx, func() (bool, error) {
		result, err := admin.ListConsumerGroupOffsets(s.group, map[string][]int32{s.topic: {partition}})
		if err != nil {
			return false, err
		}
		block := result.GetBlock(s.topic, partition)
		if block == nil {
			return false, nil
		}
		return block.Offset > replayOffset, nil
	})
}
