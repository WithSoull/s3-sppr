CREATE TABLE ingestion_streams (
    gateway_id text NOT NULL,
    stream_id text NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL CHECK (partition >= 0),
    last_sequence bigint NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    first_event_at timestamptz,
    last_event_at timestamptz,
    missing_events bigint NOT NULL DEFAULT 0 CHECK (missing_events >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (gateway_id, stream_id)
);
CREATE TABLE access_events (
    gateway_id text NOT NULL,
    stream_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    occurred_at timestamptz NOT NULL,
    bucket text NOT NULL,
    object_key text NOT NULL,
    version_id text NOT NULL,
    operation text NOT NULL CHECK (operation IN ('read','write')),
    origin text NOT NULL CHECK (origin IN ('client','migration')),
    result text NOT NULL CHECK (result IN ('success','error')),
    bytes bigint NOT NULL CHECK (bytes >= 0),
    payload_hash bytea NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (gateway_id, stream_id, sequence),
    FOREIGN KEY (gateway_id, stream_id) REFERENCES ingestion_streams,
    UNIQUE (topic, partition, kafka_offset)
);
CREATE INDEX access_events_version_time ON access_events (bucket, object_key, version_id, occurred_at);
CREATE TABLE ingestion_gaps (
    gateway_id text NOT NULL,
    stream_id text NOT NULL,
    first_sequence bigint NOT NULL CHECK (first_sequence > 0),
    last_sequence bigint NOT NULL CHECK (last_sequence >= first_sequence),
    after_time timestamptz,
    before_time timestamptz NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (gateway_id, stream_id, first_sequence),
    FOREIGN KEY (gateway_id, stream_id) REFERENCES ingestion_streams
);
CREATE TABLE read_aggregates (
    bucket text NOT NULL,
    object_key text NOT NULL,
    version_id text NOT NULL,
    minute timestamptz NOT NULL,
    reads bigint NOT NULL CHECK (reads > 0),
    bytes bigint NOT NULL CHECK (bytes >= 0),
    PRIMARY KEY (bucket, object_key, version_id, minute)
);
