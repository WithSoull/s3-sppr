-- The old schema cannot represent an unknown event count. Preserve evidence
-- instead of silently deleting those gaps or inventing a finite count.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM ingestion_gaps WHERE last_sequence IS NULL) THEN
  RAISE EXCEPTION 'cannot downgrade history completeness while unknown-count gaps exist';
 END IF;
END; $$;
DROP INDEX ingestion_streams_open;
DROP TABLE ingestion_heartbeats;
ALTER TABLE ingestion_gaps ALTER COLUMN last_sequence SET NOT NULL;
ALTER TABLE ingestion_streams DROP COLUMN complete_until, DROP COLUMN last_heartbeat_at, DROP COLUMN first_heartbeat_at,
 DROP COLUMN closed_at, DROP COLUMN lost_at, DROP COLUMN created_at;
