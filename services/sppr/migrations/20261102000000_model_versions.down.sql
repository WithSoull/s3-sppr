DELETE FROM admin_commands WHERE type = 'retrain';
ALTER TABLE admin_commands DROP CONSTRAINT admin_commands_type_check;
ALTER TABLE admin_commands ADD CONSTRAINT admin_commands_type_check CHECK
 (type IN ('confirm','reject','cancel_pending','pause','resume','settings','recompute'));
DROP TABLE model_versions;
