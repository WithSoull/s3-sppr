-- Refuse downgrade if the old key cannot represent both gaps. Never erase evidence.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM ingestion_gaps GROUP BY gateway_id,stream_id,first_sequence HAVING count(*)>1) THEN
  RAISE EXCEPTION 'cannot downgrade: lost and sequence gaps share a position';
 END IF;
END; $$;
ALTER TABLE ingestion_gaps DROP CONSTRAINT ingestion_gaps_pkey;
ALTER TABLE ingestion_gaps ADD PRIMARY KEY (gateway_id,stream_id,first_sequence);
ALTER TABLE ingestion_gaps DROP COLUMN is_lost;
ALTER TABLE ingestion_streams DROP COLUMN resumed_count, DROP COLUMN last_resumed_at;
