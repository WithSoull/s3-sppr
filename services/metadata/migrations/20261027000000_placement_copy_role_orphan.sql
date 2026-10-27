-- +goose Up

-- A separate file: PostgreSQL does not let a transaction use an enum value it
-- added, and the next migration's CHECK refers to 'orphan'.
ALTER TYPE placement_copy_role ADD VALUE 'orphan';

-- +goose Down

-- Enum values cannot be dropped; recreate the type without 'orphan'.
ALTER TABLE placement_copies DROP CONSTRAINT chk_placement_copies_target_precondition;
ALTER TYPE placement_copy_role RENAME TO placement_copy_role_old;
CREATE TYPE placement_copy_role AS ENUM (
    'target',
    'source'
);
ALTER TABLE placement_copies
    ALTER COLUMN role TYPE placement_copy_role USING role::text::placement_copy_role;
DROP TYPE placement_copy_role_old;
ALTER TABLE placement_copies ADD CONSTRAINT chk_placement_copies_target_precondition
    CHECK ((role = 'target') = (expected_generation IS NOT NULL AND expected_class IS NOT NULL));
