-- A dependent action waits until its predecessor's source blob is deleted:
-- before that the blob still occupies the class the dependent moves into.
ALTER TABLE migration_actions ADD COLUMN source_cleaned_at timestamptz;
UPDATE migration_actions SET source_cleaned_at = finished_at WHERE status = 'succeeded';
