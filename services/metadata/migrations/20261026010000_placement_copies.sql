-- +goose Up

CREATE TYPE placement_copy_role AS ENUM (
    'target',
    'source'
);

-- reserved: target blob is being written and verified.
-- promoted: target became the placement after CAS (counted via version_placements).
-- cleanup_pending, cleanup_failed: blob must be deleted from storage.
-- deleted: blob removed, space released.
CREATE TYPE placement_copy_state AS ENUM (
    'reserved',
    'promoted',
    'cleanup_pending',
    'cleanup_failed',
    'deleted'
);

-- Blobs of a version outside its placement. Rows outlive the version
-- (version_id becomes NULL) so a deleted object cannot hide an orphan blob.
CREATE TABLE placement_copies (
    copy_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_id        UUID NOT NULL,
    version_id          UUID NULL REFERENCES versions(id) ON DELETE SET NULL,
    role                placement_copy_role NOT NULL,
    storage_class       TEXT NOT NULL REFERENCES storage_classes(name),
    blob_id             UUID NOT NULL DEFAULT gen_random_uuid(),
    size_bytes          BIGINT NOT NULL CHECK (size_bytes >= 0),
    -- CAS precondition captured when the target was reserved.
    expected_generation BIGINT NULL,
    expected_class      TEXT NULL REFERENCES storage_classes(name),
    state               placement_copy_state NOT NULL,
    verified_md5        TEXT NOT NULL DEFAULT '',
    cleanup_after       TIMESTAMPTZ NULL,
    cleanup_attempts    INT NOT NULL DEFAULT 0,
    last_error          TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (migration_id, role),
    CONSTRAINT chk_placement_copies_target_precondition
        CHECK ((role = 'target') = (expected_generation IS NOT NULL AND expected_class IS NOT NULL)),
    CONSTRAINT chk_placement_copies_cleanup_after
        CHECK (state NOT IN ('cleanup_pending', 'cleanup_failed') OR cleanup_after IS NOT NULL)
);

CREATE UNIQUE INDEX ux_placement_copies_one_reserved_per_version
    ON placement_copies(version_id)
    WHERE state = 'reserved';

CREATE INDEX ON placement_copies(storage_class, state);
CREATE INDEX ON placement_copies(cleanup_after)
    WHERE state IN ('cleanup_pending', 'cleanup_failed');

-- +goose Down

DROP TABLE IF EXISTS placement_copies;
DROP TYPE IF EXISTS placement_copy_state;
DROP TYPE IF EXISTS placement_copy_role;
