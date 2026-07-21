-- Dev tooling only: runtime migrations are forward-only — a DB newer than
-- the binary hard-fails rather than downgrading.
-- Drops 0001_init in reverse dependency order.

BEGIN IMMEDIATE;

DROP TABLE IF EXISTS kv;
DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS endpoint_cooldowns;
DROP INDEX IF EXISTS idx_reference_files_size;
DROP TABLE IF EXISTS reference_files;
DROP TABLE IF EXISTS upstreams;
DROP INDEX IF EXISTS idx_blocks_lease;
DROP TABLE IF EXISTS blocks;
DROP TABLE IF EXISTS job_files;
DROP INDEX IF EXISTS idx_files_status;
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS repos;

COMMIT;
