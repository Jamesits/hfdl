-- 0002: blocks.last_error — records the failure cause of the last requeue
-- (per-item exponential backoff: available_at hides the row, retries++). DDL only.

BEGIN IMMEDIATE;

ALTER TABLE blocks ADD COLUMN last_error TEXT;

COMMIT;
