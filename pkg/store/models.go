package store

import (
	"time"

	"github.com/uptrace/bun"
)

// Status enums mirror the schema CHECK constraints. Guards make every
// transition a single statement, so a crash between steps can never produce
// a state outside the machine.
type (
	RepoStatus      string // pending → listing → listed | error
	JobStatus       string // queued → running → done | error
	FileStatus      string // discovered → (salvaging) → queued → downloading → downloaded → verifying → cached | error
	JobFileStatus   string // pending → installing → done | error
	BlockStatus     string // pending → active → done
	ReferenceStatus string // pending → hashing → hashed | error
)

const (
	RepoPending RepoStatus = "pending"
	RepoListing RepoStatus = "listing"
	RepoListed  RepoStatus = "listed"
	RepoError   RepoStatus = "error"

	JobQueued  JobStatus = "queued"
	JobRunning JobStatus = "running"
	JobDone    JobStatus = "done"
	JobError   JobStatus = "error"

	FileDiscovered  FileStatus = "discovered"
	FileSalvaging   FileStatus = "salvaging"
	FileQueued      FileStatus = "queued"
	FileDownloading FileStatus = "downloading"
	FileDownloaded  FileStatus = "downloaded"
	FileVerifying   FileStatus = "verifying"
	FileCached      FileStatus = "cached"
	FileError       FileStatus = "error"

	JobFilePending    JobFileStatus = "pending"
	JobFileInstalling JobFileStatus = "installing"
	JobFileDone       JobFileStatus = "done"
	JobFileError      JobFileStatus = "error"

	BlockPending BlockStatus = "pending"
	BlockActive  BlockStatus = "active"
	BlockDone    BlockStatus = "done"

	RefPending ReferenceStatus = "pending"
	RefHashing ReferenceStatus = "hashing"
	RefHashed  ReferenceStatus = "hashed"
	RefError   ReferenceStatus = "error"
)

// DestMode values of jobs.dest_mode.
const (
	DestModeCache    = "cache"
	DestModeLocalDir = "local-dir"
)

// Cooldown kinds of endpoint_cooldowns.kind.
const (
	CooldownAPI = "api"
	CooldownCAS = "cas"
)

// FileEntry is the listing input persisted by CompleteListing. It mirrors
// hfapi.FileEntry field-for-field (store must not import hfapi; sched maps).
type FileEntry struct {
	Path    string
	Size    int64
	GitOID  string // tree oid: git blob sha1 (the LFS pointer blob for LFS files)
	SHA256  string // lfs.sha256: raw content sha256; empty for non-LFS
	XetHash string // xet CAS reconstruction id; NEVER a verify target
	IsLFS   bool
}

type Repo struct {
	bun.BaseModel `bun:"table:repos,alias:r"`

	ID          int64      `bun:"id,pk,autoincrement"`
	Type        string     `bun:"type,notnull"`
	Name        string     `bun:"name,notnull"` // org/repo
	Revision    string     `bun:"revision,notnull"`
	CommitSHA   string     `bun:"commit_sha,nullzero"` // resolved once, then immutable
	Endpoint    string     `bun:"endpoint,notnull"`
	Status      RepoStatus `bun:"status,notnull"`
	Retries     int        `bun:"retries,notnull"`
	LastError   string     `bun:"last_error,nullzero"`
	AvailableAt *time.Time `bun:"available_at"` // durable meta-retry backoff
	LeaseOwner  string     `bun:"lease_owner,nullzero"`
	LeaseToken  string     `bun:"lease_token,nullzero"`
	LeaseUntil  *time.Time `bun:"lease_until"`
	CreatedAt   time.Time  `bun:"created_at,notnull"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull"`
}

type Job struct {
	bun.BaseModel `bun:"table:jobs,alias:j"`

	ID        int64     `bun:"id,pk,autoincrement"`
	RepoID    int64     `bun:"repo_id,notnull"`
	Filenames string    `bun:"filenames,nullzero"` // JSON positional files; "" = snapshot mode
	Include   string    `bun:"include,nullzero"`   // JSON globs (logging.JSONValue form)
	Exclude   string    `bun:"exclude,nullzero"`
	DestMode  string    `bun:"dest_mode,notnull"` // DestMode*
	DestDir   string    `bun:"dest_dir,notnull"`
	Status    JobStatus `bun:"status,notnull"`
	LastError string    `bun:"last_error,nullzero"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`

	// Repo carries the repo row EnqueueJob upserts; not scanned unless the
	// query explicitly asks for the relation.
	Repo *Repo `bun:"rel:belongs-to,join:repo_id=id"`
}

type File struct {
	bun.BaseModel `bun:"table:files,alias:f"`

	ID          int64      `bun:"id,pk,autoincrement"`
	RepoID      int64      `bun:"repo_id,notnull"`
	Path        string     `bun:"path,notnull"`
	Size        int64      `bun:"size,notnull"` // -1 = unknown until listed
	GitOID      string     `bun:"git_oid,nullzero"`
	SHA256      string     `bun:"sha256,nullzero"`
	XetHash     string     `bun:"xet_hash,nullzero"`
	IsLFS       bool       `bun:"is_lfs,notnull"`
	Status      FileStatus `bun:"status,notnull"`
	CachePath   string     `bun:"cache_path,nullzero"`
	BlockSize   int64      `bun:"block_size,notnull"` // 0 = adaptive
	Conns       int        `bun:"conns,notnull"`
	Progress    []byte     `bun:"progress"` // fsynced-bytes IntervalSet; owned by transfer
	ProgressVer int        `bun:"progress_ver,notnull"`
	Retries     int        `bun:"retries,notnull"`
	VerifyFails int        `bun:"verify_fails,notnull"`
	LastError   string     `bun:"last_error,nullzero"`
	LeaseOwner  string     `bun:"lease_owner,nullzero"`
	LeaseToken  string     `bun:"lease_token,nullzero"`
	LeaseUntil  *time.Time `bun:"lease_until"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull"`
}

type JobFile struct {
	bun.BaseModel `bun:"table:job_files,alias:jf"`

	JobID      int64         `bun:"job_id,pk"`
	FileID     int64         `bun:"file_id,pk"`
	DestPath   string        `bun:"dest_path,nullzero"` // resolved install target for this job
	Status     JobFileStatus `bun:"status,notnull"`
	LastError  string        `bun:"last_error,nullzero"`
	LeaseOwner string        `bun:"lease_owner,nullzero"`
	LeaseToken string        `bun:"lease_token,nullzero"`
	LeaseUntil *time.Time    `bun:"lease_until"`
	UpdatedAt  time.Time     `bun:"updated_at,notnull"`
}

type Block struct {
	bun.BaseModel `bun:"table:blocks,alias:b"`

	ID          int64       `bun:"id,pk,autoincrement"`
	FileID      int64       `bun:"file_id,notnull"`
	Idx         int         `bun:"idx,notnull"`
	Offset      int64       `bun:"offset,notnull"`
	Length      int64       `bun:"length,notnull"`
	Status      BlockStatus `bun:"status,notnull"`
	Upstream    string      `bun:"upstream,nullzero"`
	Retries     int         `bun:"retries,notnull"`
	LastError   string      `bun:"last_error,nullzero"` // cause of the last requeue
	AvailableAt *time.Time  `bun:"available_at"`        // durable retry backoff
	LeaseOwner  string      `bun:"lease_owner,nullzero"`
	LeaseToken  string      `bun:"lease_token,nullzero"`
	LeaseUntil  *time.Time  `bun:"lease_until"`
	UpdatedAt   time.Time   `bun:"updated_at,notnull"`
}

type Upstream struct {
	bun.BaseModel `bun:"table:upstreams,alias:u"`

	ID             int64      `bun:"id,pk,autoincrement"`
	Endpoint       string     `bun:"endpoint,notnull"`
	CooldownUntil  *time.Time `bun:"cooldown_until"`  // per-upstream 429/503 gate
	EmaBps         float64    `bun:"ema_bps,notnull"` // speed signal for upstream policy
	Errors         int64      `bun:"errors,notnull"`
	Successes      int64      `bun:"successes,notnull"`
	BlacklistUntil *time.Time `bun:"blacklist_until"`
	UpdatedAt      time.Time  `bun:"updated_at,notnull"`
}

type ReferenceFile struct {
	bun.BaseModel `bun:"table:reference_files,alias:rf"`

	ID         int64           `bun:"id,pk,autoincrement"`
	Path       string          `bun:"path,notnull"`
	Size       int64           `bun:"size,notnull"`
	MtimeNs    int64           `bun:"mtime_ns,notnull"` // invalidation key: size+mtime_ns+dev+ino
	Dev        uint64          `bun:"dev,notnull"`
	Ino        uint64          `bun:"ino,notnull"`
	Status     ReferenceStatus `bun:"status,notnull"`
	SHA256     string          `bun:"sha256,nullzero"`
	LeaseOwner string          `bun:"lease_owner,nullzero"`
	LeaseToken string          `bun:"lease_token,nullzero"`
	LeaseUntil *time.Time      `bun:"lease_until"`
	LastError  string          `bun:"last_error,nullzero"`
	UpdatedAt  time.Time       `bun:"updated_at,notnull"`
}

type EndpointCooldown struct {
	bun.BaseModel `bun:"table:endpoint_cooldowns,alias:ec"`

	Endpoint string    `bun:"endpoint,pk"`
	Kind     string    `bun:"kind,pk"` // Cooldown*
	Until    time.Time `bun:"until,notnull"`
	Reason   string    `bun:"reason,nullzero"`
}

type Log struct {
	bun.BaseModel `bun:"table:logs,alias:l"`

	ID     int64     `bun:"id,pk,autoincrement"`
	Ts     time.Time `bun:"ts,notnull"`
	Level  int       `bun:"level,notnull"` // slog.Level numeric value
	Source string    `bun:"source,nullzero"`
	Msg    string    `bun:"msg,notnull"`
	Attrs  string    `bun:"attrs,nullzero"` // JSON-encoded attrs
}

type KV struct {
	bun.BaseModel `bun:"table:kv,alias:kv"`

	Key   string `bun:"key,pk"`
	Value string `bun:"value,notnull"`
}
