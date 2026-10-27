-- +goose Up

-- orphan: blob of a version whose object was deleted. It belongs to no migration
-- and waits in cleanup_pending until reads started before the delete finish.
ALTER TABLE placement_copies ALTER COLUMN migration_id DROP NOT NULL;
ALTER TABLE placement_copies ADD CONSTRAINT chk_placement_copies_orphan_migration
    CHECK ((role = 'orphan') = (migration_id IS NULL));

-- +goose Down

-- Rolling back forgets orphan blobs that are still on the Storage volumes.
DELETE FROM placement_copies WHERE role = 'orphan';
ALTER TABLE placement_copies DROP CONSTRAINT chk_placement_copies_orphan_migration;
ALTER TABLE placement_copies ALTER COLUMN migration_id SET NOT NULL;
