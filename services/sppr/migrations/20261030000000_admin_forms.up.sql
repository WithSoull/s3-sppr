CREATE TABLE admin_form_commands (
 command_id uuid PRIMARY KEY,
 initiator text NOT NULL,
 payload text NOT NULL,
 command jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX admin_form_commands_created ON admin_form_commands(created_at DESC,command_id);
