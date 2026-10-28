CREATE TABLE model_versions (
 model_version text PRIMARY KEY,
 status text NOT NULL CHECK (status IN ('candidate','active','rejected','retired')),
 reason text NOT NULL DEFAULT '',
 source text NOT NULL CHECK (source IN ('bootstrap','scheduled','manual')),
 trained_from timestamptz, trained_to timestamptz,
 train_rows bigint NOT NULL DEFAULT 0 CHECK (train_rows >= 0),
 train_seconds double precision NOT NULL DEFAULT 0 CHECK (train_seconds >= 0),
 validation_cov_k double precision, validation_ewma_cov_k double precision,
 created_at timestamptz NOT NULL, activated_at timestamptz, retired_at timestamptz
);
CREATE UNIQUE INDEX model_versions_one_active ON model_versions ((status)) WHERE status = 'active';
CREATE INDEX model_versions_created ON model_versions (created_at DESC, model_version DESC);
ALTER TABLE admin_commands DROP CONSTRAINT admin_commands_type_check;
ALTER TABLE admin_commands ADD CONSTRAINT admin_commands_type_check CHECK
 (type IN ('confirm','reject','cancel_pending','pause','resume','settings','recompute','retrain'));
