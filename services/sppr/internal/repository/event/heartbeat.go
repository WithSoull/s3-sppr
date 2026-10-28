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

func (r *repo) StoreHeartbeat(ctx context.Context, h model.StreamHeartbeat, d model.Delivery) (model.IngestResult, error) {
	if _, ok := txctx.ExtractTx(ctx); !ok {
		return model.IngestResult{}, fmt.Errorf("heartbeat storage requires a transaction")
	}
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "heartbeat:ensure", QueryRaw: `INSERT INTO ingestion_streams(gateway_id,stream_id,topic,partition) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`}, h.GatewayID, h.StreamID, d.Topic, d.Partition)
	if err != nil {
		return model.IngestResult{}, err
	}
	var seq, priorMissing int64
	var last, heartbeat, closed, lost *time.Time
	var topic string
	var partition int32
	err = r.db.DB().QueryRowContext(ctx, db.Query{Name: "heartbeat:lock", QueryRaw: `SELECT last_sequence,last_event_at,last_heartbeat_at,closed_at,topic,partition,missing_events,lost_at FROM ingestion_streams WHERE gateway_id=$1 AND stream_id=$2 FOR UPDATE`}, h.GatewayID, h.StreamID).Scan(&seq, &last, &heartbeat, &closed, &topic, &partition, &priorMissing, &lost)
	if err != nil {
		return model.IngestResult{}, err
	}
	if topic != d.Topic || partition != d.Partition {
		return model.IngestResult{}, fmt.Errorf("%w: stream moved", model.ErrRejected)
	}
	hash := sha256.Sum256(h.Payload)
	var previous []byte
	err = r.db.DB().QueryRowContext(ctx, db.Query{Name: "heartbeat:replay", QueryRaw: `SELECT payload_hash FROM ingestion_heartbeats WHERE (topic=$1 AND partition=$2 AND kafka_offset=$3) OR (gateway_id=$4 AND stream_id=$5 AND payload_hash=$6) ORDER BY (topic=$1 AND partition=$2 AND kafka_offset=$3) DESC LIMIT 1`}, d.Topic, d.Partition, d.Offset, h.GatewayID, h.StreamID, hash[:]).Scan(&previous)
	if err == nil {
		if !bytes.Equal(previous, hash[:]) {
			return model.IngestResult{}, fmt.Errorf("%w: heartbeat delivery reused", model.ErrRejected)
		}
		return model.IngestResult{Duplicate: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return model.IngestResult{}, err
	}

	missing, complete, err := heartbeatProgress(seq, last, heartbeat, closed, lost, h, priorMissing)
	if err != nil {
		return model.IngestResult{}, err
	}
	resumed, err := r.resume(ctx, h.GatewayID, h.StreamID, seq, lost, h.Sequence == seq)
	if err != nil {
		return model.IngestResult{}, err
	}
	if missing > 0 {
		_, err = r.db.DB().ExecContext(ctx, db.Query{Name: "heartbeat:gap", QueryRaw: `INSERT INTO ingestion_gaps(gateway_id,stream_id,first_sequence,last_sequence,after_time,before_time) VALUES($1,$2,$3,$4,$5,$6)`}, h.GatewayID, h.StreamID, seq+1, h.Sequence, last, h.At)
	}
	if err != nil {
		return model.IngestResult{}, err
	}
	_, err = r.db.DB().ExecContext(ctx, db.Query{Name: "heartbeat:progress", QueryRaw: `UPDATE ingestion_streams SET last_sequence=$3,complete_until=$4,last_heartbeat_at=$5,first_heartbeat_at=COALESCE(first_heartbeat_at,$5),closed_at=CASE WHEN $6 THEN $5 ELSE NULL END,missing_events=missing_events+$7,updated_at=now() WHERE gateway_id=$1 AND stream_id=$2`}, h.GatewayID, h.StreamID, h.Sequence, complete, h.At, h.Closing, missing)
	if err != nil {
		return model.IngestResult{}, err
	}
	_, err = r.db.DB().ExecContext(ctx, db.Query{Name: "heartbeat:receipt", QueryRaw: `INSERT INTO ingestion_heartbeats(topic,partition,kafka_offset,gateway_id,stream_id,payload_hash) VALUES($1,$2,$3,$4,$5,$6)`}, d.Topic, d.Partition, d.Offset, h.GatewayID, h.StreamID, hash[:])
	return model.IngestResult{Missing: missing, Resumed: resumed}, err
}
func (r *repo) CloseLostStreams(ctx context.Context, now time.Time, after time.Duration) error {
	if _, ok := txctx.ExtractTx(ctx); !ok {
		return fmt.Errorf("lost stream storage requires a transaction")
	}
	// The UPDATE and gap insertion share one statement/transaction and the same
	// row locks as event/heartbeat ingestion. No liveness refresh from read traffic.
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "heartbeat:lost", QueryRaw: `WITH lost AS (
 UPDATE ingestion_streams SET lost_at=$1,closed_at=$1,updated_at=$1
 WHERE closed_at IS NULL AND COALESCE(last_heartbeat_at,created_at)<$2
 RETURNING gateway_id,stream_id,last_sequence,COALESCE(complete_until,first_event_at,created_at) AS gap_start
 ) INSERT INTO ingestion_gaps(gateway_id,stream_id,first_sequence,last_sequence,after_time,before_time,is_lost)
 SELECT gateway_id,stream_id,last_sequence+1,NULL,gap_start,$1,true FROM lost`}, now, now.Add(-after))
	return err
}

func heartbeatProgress(seq int64, last, heartbeat, closed, lost *time.Time, h model.StreamHeartbeat, priorMissing int64) (int64, *time.Time, error) {
	if (closed != nil && lost == nil) || h.Sequence < seq || (last != nil && h.At.Before(*last)) || (heartbeat != nil && h.At.Before(*heartbeat)) {
		return 0, nil, fmt.Errorf("%w: heartbeat regressed or stream closed", model.ErrRejected)
	}
	missing := h.Sequence - seq
	if missing > 0 || priorMissing > 0 {
		return missing, last, nil
	}
	return 0, &h.At, nil
}
