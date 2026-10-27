CREATE TABLE migration_actions (
    action_id uuid PRIMARY KEY,
    plan_id uuid NOT NULL,
    version_id uuid NOT NULL,
    expected_generation bigint NOT NULL CHECK (expected_generation > 0),
    source_class text NOT NULL CHECK (source_class IN ('HOT','COLD')),
    target_class text NOT NULL CHECK (target_class IN ('HOT','COLD')),
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','succeeded','failed','cancelled')),
    step text CHECK (step IN ('reserving','copying','switching','aborting')),
    reason text,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    bytes_charged bigint NOT NULL DEFAULT 0 CHECK (bytes_charged >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (source_class <> target_class),
    CHECK ((status = 'running') = (step IS NOT NULL)),
    -- An aborting action keeps the reason it will fail with, so a restart can finish it.
    CHECK ((status IN ('failed','cancelled') OR COALESCE(step, '') = 'aborting') = (reason IS NOT NULL))
);
CREATE INDEX migration_actions_claim ON migration_actions (created_at, action_id) WHERE status = 'pending';
CREATE INDEX migration_actions_running ON migration_actions (action_id) WHERE status = 'running';
CREATE INDEX migration_actions_plan ON migration_actions (plan_id, action_id);

CREATE TABLE migration_action_deps (
    action_id uuid NOT NULL REFERENCES migration_actions ON DELETE CASCADE,
    depends_on uuid NOT NULL REFERENCES migration_actions,
    PRIMARY KEY (action_id, depends_on),
    CHECK (action_id <> depends_on)
);
CREATE INDEX migration_action_deps_reverse ON migration_action_deps (depends_on);

CREATE TABLE migration_action_events (
    id bigserial PRIMARY KEY,
    action_id uuid NOT NULL REFERENCES migration_actions ON DELETE CASCADE,
    at timestamptz NOT NULL DEFAULT now(),
    from_status text,
    to_status text NOT NULL,
    from_step text,
    to_step text,
    bytes bigint NOT NULL DEFAULT 0 CHECK (bytes >= 0),
    reason text,
    error text
);
CREATE INDEX migration_action_events_action ON migration_action_events (action_id, id);

-- One row per copy attempt. The row is opened with the planned size when the
-- attempt is reserved and settled with the bytes actually transferred.
CREATE TABLE migration_budget_ledger (
    action_id uuid NOT NULL REFERENCES migration_actions ON DELETE CASCADE,
    attempt integer NOT NULL CHECK (attempt > 0),
    bytes bigint NOT NULL CHECK (bytes >= 0),
    settled boolean NOT NULL DEFAULT false,
    charged_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (action_id, attempt)
);
CREATE INDEX migration_budget_ledger_time ON migration_budget_ledger (charged_at);
CREATE UNIQUE INDEX migration_budget_ledger_open ON migration_budget_ledger (action_id) WHERE NOT settled;

CREATE TABLE migration_settings (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    budget_bytes bigint NOT NULL CHECK (budget_bytes > 0),
    budget_window_seconds bigint NOT NULL CHECK (budget_window_seconds > 0),
    min_interval_seconds bigint NOT NULL CHECK (min_interval_seconds >= 0),
    paused boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE migration_pins (
    bucket text NOT NULL,
    object_key text NOT NULL,
    reason text NOT NULL DEFAULT '',
    author text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket, object_key)
);
