-- Applied by bun/migrate (embedded files, auto-run in Store.Open); bookkeeping
-- lives in bun's own bun_migrations / bun_migration_locks tables. This is
-- 0001_init — DDL ONLY: journal_mode=WAL is set once in Open before
-- migrating; busy_timeout / foreign_keys / synchronous are per-connection
-- DSN options: connection-scoped PRAGMAs must configure every pooled
-- connection, so they can never be migration statements.
--
-- The explicit transaction makes the multi-statement file atomic under
-- autocommit (bun runs non-.tx files on a bare connection; SQLite DDL is
-- transactional, so a failure anywhere rolls the whole migration back).

BEGIN IMMEDIATE;

CREATE TABLE repos (                         -- one row per (repo, revision) snapshot
  id          INTEGER PRIMARY KEY,
  type        TEXT NOT NULL DEFAULT 'model',
  name        TEXT NOT NULL,                 -- org/repo
  revision    TEXT NOT NULL DEFAULT 'main',  -- as requested
  commit_sha  TEXT,                          -- resolved once, then immutable
  endpoint    TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending'
              CHECK (status IN ('pending','listing','listed','error')),
  retries     INTEGER NOT NULL DEFAULT 0,
  last_error  TEXT,
  lease_owner TEXT, lease_token TEXT, lease_until TIMESTAMP,
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (type, name, revision, endpoint)
);

CREATE TABLE jobs (                          -- one row per CLI invocation: selection + destination
  id         INTEGER PRIMARY KEY,
  repo_id    INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
  filenames  TEXT,                           -- JSON: explicit positional files; NULL = snapshot mode
  include    TEXT, exclude TEXT,             -- JSON globs (logging.JSONValue)
  dest_mode  TEXT NOT NULL CHECK (dest_mode IN ('cache','local-dir')),
  dest_dir   TEXT NOT NULL,
  status     TEXT NOT NULL DEFAULT 'queued'
             CHECK (status IN ('queued','running','done','error')),
  last_error TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE files (                         -- content/download state, deduplicated per (repo, path)
  id         INTEGER PRIMARY KEY,
  repo_id    INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
  path       TEXT NOT NULL,
  size       INTEGER NOT NULL DEFAULT -1,    -- -1 = unknown until listed
  git_oid    TEXT,                           -- tree oid: git blob sha1 (pointer blob for LFS)
  sha256     TEXT,                           -- lfs.sha256; empty for non-LFS
  xet_hash   TEXT,                           -- xet CAS reconstruction id; NEVER a verify target
  is_lfs     INTEGER NOT NULL DEFAULT 0,
  -- blob id (cache key + verify target) = is_lfs ? sha256 : git_oid
  status     TEXT NOT NULL DEFAULT 'discovered'
             CHECK (status IN ('discovered','salvaging','queued','downloading','downloaded',
                               'verifying','cached','error')),
  cache_path TEXT,
  block_size INTEGER NOT NULL DEFAULT 0,     -- 0 = adaptive
  conns      INTEGER NOT NULL DEFAULT 8,     -- hot-updatable target
  progress   BLOB,                           -- durable-only IntervalSet snapshot: fsynced bytes, nothing else
  progress_ver INTEGER NOT NULL DEFAULT 1,
  retries    INTEGER NOT NULL DEFAULT 0,
  verify_fails INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  lease_owner TEXT, lease_token TEXT, lease_until TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (repo_id, path)
);
CREATE INDEX idx_files_status ON files(status, id);

CREATE TABLE job_files (                     -- association: which jobs want which files, and where
  job_id     INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
  dest_path  TEXT,                           -- resolved install target for this job
  status     TEXT NOT NULL DEFAULT 'pending'
             CHECK (status IN ('pending','installing','done','error')),
  last_error TEXT,
  lease_owner TEXT, lease_token TEXT, lease_until TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (job_id, file_id)
);

CREATE TABLE blocks (                        -- ephemeral scheduling view; byte truth lives in files.progress
  id         INTEGER PRIMARY KEY,
  file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
  idx        INTEGER NOT NULL,
  offset     INTEGER NOT NULL,
  length     INTEGER NOT NULL,
  status     TEXT NOT NULL DEFAULT 'pending'
             CHECK (status IN ('pending','active','done')),
  upstream   TEXT,
  retries    INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMP,                    -- durable retry backoff: requeued rows stay hidden until this time
  lease_owner TEXT, lease_token TEXT, lease_until TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (file_id, idx)
);
CREATE INDEX idx_blocks_lease ON blocks(status, available_at, lease_until);

CREATE TABLE upstreams (
  id             INTEGER PRIMARY KEY,
  endpoint       TEXT UNIQUE NOT NULL,
  cooldown_until TIMESTAMP,                  -- per-upstream 429/503 gate
  ema_bps        REAL NOT NULL DEFAULT 0,
  errors         INTEGER NOT NULL DEFAULT 0,
  successes      INTEGER NOT NULL DEFAULT 0,
  blacklist_until TIMESTAMP,
  updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  -- range capability is per (upstream, file), proven per run by that
  -- file's first validated 206 — deliberately not a persisted endpoint property
);

CREATE TABLE reference_files (               -- --reference roots: whole-file sha256 salvage;
                                             -- stat-only until size matches a pending target
  id          INTEGER PRIMARY KEY,
  path        TEXT UNIQUE NOT NULL,
  size        INTEGER NOT NULL,
  mtime_ns    INTEGER NOT NULL,              -- invalidation key: size+mtime_ns+dev+ino
  dev         INTEGER NOT NULL DEFAULT 0,
  ino         INTEGER NOT NULL DEFAULT 0,
  status      TEXT NOT NULL DEFAULT 'pending'
              CHECK (status IN ('pending','hashing','hashed','error')),
  sha256      TEXT,
  lease_owner TEXT, lease_token TEXT, lease_until TIMESTAMP,
  last_error  TEXT,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_reference_files_size ON reference_files(size, status); -- size-gate join against same-sized pending files

CREATE TABLE endpoint_cooldowns (            -- 429 gates, scoped per endpoint and protocol stage
  endpoint TEXT NOT NULL,
  kind     TEXT NOT NULL CHECK (kind IN ('api','cas')),
  until    TIMESTAMP NOT NULL,
  reason   TEXT,
  PRIMARY KEY (endpoint, kind)
);

CREATE TABLE logs (                          -- slog DB sink: async batched, best-effort;
  id     INTEGER PRIMARY KEY,                -- pruned to newest 50k rows at startup
  ts     TIMESTAMP NOT NULL,
  level  INTEGER NOT NULL,                   -- slog.Level numeric value
  source TEXT,                               -- component/package
  msg    TEXT NOT NULL,
  attrs  TEXT                                -- JSON-encoded attrs
);

CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL); -- HF token hash, totals,
                                                             -- volcaps:<dev> JSON, persisted limits

COMMIT;
