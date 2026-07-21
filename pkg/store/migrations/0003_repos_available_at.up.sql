-- 0003: repos.available_at — durable retry backoff for the meta queue,
-- mirroring blocks.available_at. A transient (5xx/network) listing failure
-- requeues the repo with an exponential backoff so LeaseMeta hides the row
-- until the delay elapses, instead of hot-looping lease→fail→lease. DDL only.

BEGIN IMMEDIATE;

ALTER TABLE repos ADD COLUMN available_at TIMESTAMP;

COMMIT;
