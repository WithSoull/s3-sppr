package event

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

// The caller holds the stream row lock and has validated a non-duplicate message.
func (r *repo) resume(ctx context.Context, gateway, stream string, seq int64, lost *time.Time, contiguous bool) (bool, error) {
	if lost == nil {
		return false, nil
	}
	if contiguous {
		_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "stream:resolve_lost_gap", QueryRaw: `DELETE FROM ingestion_gaps
   WHERE gateway_id=$1 AND stream_id=$2 AND first_sequence=$3
   AND is_lost AND last_sequence IS NULL AND before_time=$4`}, gateway, stream, seq+1, *lost)
		if err != nil {
			return false, err
		}
	}
	_, err := r.db.DB().ExecContext(ctx, db.Query{Name: "stream:resume", QueryRaw: `UPDATE ingestion_streams
  SET closed_at=NULL,lost_at=NULL,resumed_count=resumed_count+1,last_resumed_at=now(),updated_at=now()
  WHERE gateway_id=$1 AND stream_id=$2`}, gateway, stream)
	return err == nil, err
}
