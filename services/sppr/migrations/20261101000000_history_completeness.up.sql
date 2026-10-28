ALTER TABLE ingestion_streams
 ADD COLUMN complete_until timestamptz,
 ADD COLUMN last_heartbeat_at timestamptz,
 ADD COLUMN first_heartbeat_at timestamptz,
 ADD COLUMN closed_at timestamptz,
 ADD COLUMN lost_at timestamptz,
 ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();
-- NULL last_sequence denotes an unknown number of missing events.
ALTER TABLE ingestion_gaps ALTER COLUMN last_sequence DROP NOT NULL;
CREATE TABLE ingestion_heartbeats (
 topic text NOT NULL, partition integer NOT NULL, kafka_offset bigint NOT NULL,
 gateway_id text NOT NULL, stream_id text NOT NULL, payload_hash bytea NOT NULL,
 PRIMARY KEY(topic, partition, kafka_offset),
 FOREIGN KEY(gateway_id,stream_id) REFERENCES ingestion_streams
);
CREATE INDEX ingestion_streams_open ON ingestion_streams (last_heartbeat_at) WHERE closed_at IS NULL;
CREATE INDEX ingestion_heartbeats_replay ON ingestion_heartbeats(gateway_id,stream_id,payload_hash);
