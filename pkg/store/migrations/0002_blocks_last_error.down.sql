-- Dev tooling only. SQLite ≥3.35 supports DROP COLUMN.

BEGIN IMMEDIATE;

ALTER TABLE blocks DROP COLUMN last_error;

COMMIT;
