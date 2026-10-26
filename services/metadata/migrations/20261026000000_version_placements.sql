-- +goose Up

-- Capacities are development placeholders; experiments set their own values.
CREATE TABLE storage_classes (
    name            TEXT PRIMARY KEY,
    capacity_bytes  BIGINT NOT NULL CHECK (capacity_bytes > 0),
    max_utilization NUMERIC(4, 3) NOT NULL CHECK (max_utilization > 0 AND max_utilization <= 1)
);

INSERT INTO storage_classes (name, capacity_bytes, max_utilization) VALUES
    ('HOT', 1073741824, 0.900),
    ('COLD', 68719476736, 0.950);

-- The only location reads may use. versions.blob_id keeps the blob written on
-- upload; after a migration the placement points to a different blob.
CREATE TABLE version_placements (
    version_id    UUID PRIMARY KEY REFERENCES versions(id) ON DELETE CASCADE,
    storage_class TEXT NOT NULL REFERENCES storage_classes(name),
    blob_id       UUID NOT NULL,
    generation    BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON version_placements(storage_class);

INSERT INTO version_placements (version_id, storage_class, blob_id)
SELECT id, 'COLD', blob_id
FROM versions
WHERE kind = 'blob' AND state = 'committed' AND blob_id IS NOT NULL;

-- +goose Down

DROP TABLE IF EXISTS version_placements;
DROP TABLE IF EXISTS storage_classes;
