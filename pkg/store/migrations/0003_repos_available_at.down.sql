-- Dev tooling only. SQLite ≥3.35 supports DROP COLUMN.

BEGIN IMMEDIATE;

ALTER TABLE repos DROP COLUMN available_at;

COMMIT;
