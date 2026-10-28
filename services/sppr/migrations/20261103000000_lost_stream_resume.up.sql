ALTER TABLE ingestion_streams
 ADD COLUMN resumed_count bigint NOT NULL DEFAULT 0 CHECK (resumed_count >= 0),
 ADD COLUMN last_resumed_at timestamptz;

-- Distinguish a lost-check gap from the known sequence gap at the same position.
ALTER TABLE ingestion_gaps ADD COLUMN is_lost boolean NOT NULL DEFAULT false;
UPDATE ingestion_gaps SET is_lost=true WHERE last_sequence IS NULL;
ALTER TABLE ingestion_gaps DROP CONSTRAINT ingestion_gaps_pkey;
ALTER TABLE ingestion_gaps ADD PRIMARY KEY (gateway_id,stream_id,first_sequence,is_lost);
