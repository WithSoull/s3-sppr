-- A dependent action waits until its predecessor's source blob is deleted:
-- before that the blob still occupies the class the dependent moves into.
ALTER TABLE migration_actions ADD COLUMN source_cleaned_at timestamptz;
UPDATE migration_actions SET source_cleaned_at = finished_at WHERE status = 'succeeded';

CREATE TABLE planner_settings (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    horizon_seconds bigint NOT NULL CHECK (horizon_seconds > 0),
    hot_fixed_ms double precision NOT NULL CHECK (hot_fixed_ms >= 0),
    hot_mib_per_ms double precision NOT NULL CHECK (hot_mib_per_ms > 0),
    cold_fixed_ms double precision NOT NULL CHECK (cold_fixed_ms >= 0),
    cold_mib_per_ms double precision NOT NULL CHECK (cold_mib_per_ms > 0),
    penalty_ms_per_mib double precision NOT NULL CHECK (penalty_ms_per_mib >= 0),
    min_gain_ms double precision NOT NULL CHECK (min_gain_ms >= 0),
    hot_utilization double precision NOT NULL CHECK (hot_utilization > 0 AND hot_utilization <= 1),
    cold_utilization double precision NOT NULL CHECK (cold_utilization > 0 AND cold_utilization <= 1),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE plans (
    plan_id uuid PRIMARY KEY,
    revision bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    trigger text NOT NULL CHECK (trigger IN ('timer','manual')),
    status text NOT NULL CHECK (status IN ('computing','ready','history_incomplete','failed','superseded','confirmed','rejected')),
    method_version text NOT NULL DEFAULT '',
    params jsonb NOT NULL,
    summary jsonb NOT NULL DEFAULT '{}',
    reason text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);

CREATE TABLE recommendations (
    recommendation_id uuid PRIMARY KEY,
    plan_id uuid NOT NULL REFERENCES plans ON DELETE CASCADE,
    ord integer NOT NULL,
    kind text NOT NULL CHECK (kind IN ('promote','evict','rebalance','evict_noncurrent')),
    selected boolean NOT NULL,
    reason text NOT NULL DEFAULT '',
    version_id uuid NOT NULL,
    bucket text NOT NULL,
    object_key text NOT NULL,
    expected_generation bigint NOT NULL,
    source_class text NOT NULL CHECK (source_class IN ('HOT','COLD')),
    target_class text NOT NULL CHECK (target_class IN ('HOT','COLD')),
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    explanation jsonb NOT NULL,
    UNIQUE (plan_id, ord)
);

CREATE TABLE recommendation_deps (
    recommendation_id uuid NOT NULL REFERENCES recommendations ON DELETE CASCADE,
    depends_on uuid NOT NULL REFERENCES recommendations ON DELETE CASCADE,
    PRIMARY KEY (recommendation_id, depends_on)
);

-- Every administrator decision, including rejected ones; request is the
-- normalized command a retry with the same command_id must repeat.
CREATE TABLE admin_commands (
    command_id uuid PRIMARY KEY,
    type text NOT NULL CHECK (type IN ('confirm','reject','cancel_pending','pause','resume','settings','recompute')),
    initiator text NOT NULL,
    plan_id uuid,
    request jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('accepted','rejected')),
    code text NOT NULL DEFAULT '',
    message text NOT NULL DEFAULT '',
    result jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX admin_commands_created ON admin_commands (created_at DESC, command_id);
CREATE INDEX admin_commands_plan ON admin_commands (plan_id, created_at DESC);
