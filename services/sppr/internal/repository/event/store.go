package event

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/contextx/txctx"
	"github.com/jackc/pgx/v4"
)

func (r *repo) Store(ctx context.Context, e model.AccessEvent, d model.Delivery) (model.IngestResult, error) {
	if _, ok := txctx.ExtractTx(ctx); !ok {
		return model.IngestResult{}, fmt.Errorf("event storage requires a transaction")
	}
	if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "event:ensure_stream", QueryRaw: `
		INSERT INTO ingestion_streams (gateway_id, stream_id, topic, partition)
		VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`}, e.GatewayID, e.StreamID, d.Topic, d.Partition); err != nil {
		return model.IngestResult{}, err
	}
	var sequence int64
	var lastAt, closedAt, heartbeatAt *time.Time
	var topic string
	var partition int32
	err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "event:lock_stream", QueryRaw: `
		SELECT last_sequence, last_event_at, topic, partition, closed_at, last_heartbeat_at FROM ingestion_streams
		WHERE gateway_id=$1 AND stream_id=$2 FOR UPDATE`}, e.GatewayID, e.StreamID).
		Scan(&sequence, &lastAt, &topic, &partition, &closedAt, &heartbeatAt)
	if err != nil {
		return model.IngestResult{}, err
	}
	if topic != d.Topic || partition != d.Partition {
		return model.IngestResult{}, fmt.Errorf("%w: stream moved to another topic or partition", model.ErrRejected)
	}
	hash := sha256.Sum256(e.Payload)
	if e.Sequence <= sequence {
		var previous []byte
		err := r.db.DB().QueryRowContext(ctx, db.Query{Name: "event:duplicate", QueryRaw: `
			SELECT payload_hash FROM access_events WHERE gateway_id=$1 AND stream_id=$2 AND sequence=$3`},
			e.GatewayID, e.StreamID, e.Sequence).Scan(&previous)
		if errors.Is(err, pgx.ErrNoRows) {
			return model.IngestResult{}, fmt.Errorf("%w: late event from a recorded gap", model.ErrRejected)
		}
		if err != nil {
			return model.IngestResult{}, err
		}
		if !bytes.Equal(previous, hash[:]) {
			return model.IngestResult{}, fmt.Errorf("%w: event identity reused with different payload", model.ErrRejected)
		}
		return model.IngestResult{Duplicate: true}, nil
	}
	if closedAt != nil {
		return model.IngestResult{}, fmt.Errorf("%w: closed stream", model.ErrRejected)
	}
	if heartbeatAt != nil && e.OccurredAt.Before(*heartbeatAt) {
		return model.IngestResult{}, fmt.Errorf("%w: event before heartbeat", model.ErrRejected)
	}
	if lastAt != nil && e.OccurredAt.Before(*lastAt) {
		return model.IngestResult{}, fmt.Errorf("%w: event time regressed within stream", model.ErrRejected)
	}
	if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "event:insert", QueryRaw: `
		INSERT INTO access_events (gateway_id, stream_id, sequence, occurred_at, bucket, object_key,
		 version_id, operation, origin, result, bytes, payload_hash, topic, partition, kafka_offset)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`},
		e.GatewayID, e.StreamID, e.Sequence, e.OccurredAt, e.Bucket, e.ObjectKey, e.VersionID,
		e.Operation, e.Origin, e.Result, e.Bytes, hash[:], d.Topic, d.Partition, d.Offset); err != nil {
		return model.IngestResult{}, err
	}
	missing := e.Sequence - sequence - 1
	if missing > 0 {
		if _, err := r.db.DB().ExecContext(ctx, db.Query{Name: "event:gap", QueryRaw: `
			INSERT INTO ingestion_gaps (gateway_id, stream_id, first_sequence, last_sequence, after_time, before_time)
			VALUES ($1,$2,$3,$4,$5,$6)`}, e.GatewayID, e.StreamID, sequence+1, e.Sequence-1, lastAt, e.OccurredAt); err != nil {
			return model.IngestResult{}, err
		}
	}
	if e.IsDemand() {
		if err := r.addDemand(ctx, e); err != nil {
			return model.IngestResult{}, err
		}
	}
	_, err = r.db.DB().ExecContext(ctx, db.Query{Name: "event:progress", QueryRaw: `
		UPDATE ingestion_streams SET last_sequence=$3, last_event_at=$4,
		 first_event_at=COALESCE(first_event_at,$4), missing_events=missing_events+$5, updated_at=now()
		WHERE gateway_id=$1 AND stream_id=$2`}, e.GatewayID, e.StreamID, e.Sequence, e.OccurredAt, missing)
	return model.IngestResult{Missing: missing}, err
}

func (r *repo) addDemand(ctx context.Context, e model.AccessEvent) error {
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "event:demand", QueryRaw: `
		INSERT INTO read_aggregates (bucket, object_key, version_id, minute, reads, bytes)
		VALUES ($1,$2,$3,$4,1,$5)
		ON CONFLICT (bucket,object_key,version_id,minute) DO UPDATE
		SET reads=read_aggregates.reads+1, bytes=read_aggregates.bytes+EXCLUDED.bytes`},
		e.Bucket, e.ObjectKey, e.VersionID, e.OccurredAt.Truncate(time.Minute), e.Bytes)
	return err
}
