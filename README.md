# HFDL

![Project Status - Feature Complete](https://img.shields.io/badge/Project_Status-Feature_Complete-2ea44f)
![100% AI Code](https://img.shields.io/badge/AI_Code-100%25-blue)
![Reviewed by a Human](https://img.shields.io/badge/Reviewed_by-a_Human-green)
![Works - On My Machine](https://img.shields.io/badge/Works-On_My_Machine-2ea44f)
[![Go Reference](https://pkg.go.dev/badge/github.com/jamesits/hfdl.svg)](https://pkg.go.dev/github.com/jamesits/hfdl)

Drop-in replacement of `hf download` that:

- Does not freak out on a slow disk (even if an HDD)
- Does not consume 114514G RAM and DoS your disk for so-called "high performance" mode
- Does not get rewrited in Rust for no reason
- Has a proper TUI instead of progress bars mixed with partial logs
- Supports simultaneous downloads from multiple mirrors
- Can adjust network speed limits on the fly
- Is end-to-end OTLP traced
- And performant!

## Usage

Replace `hf` or `uvx hf` with `hfdl` for downloads:

```shell
hfdl download [args...]
```

## Feature Parity

### Command Line Arguments

Differences to `hf download`:

- `--type` not implemented, use `--repo-type` or URL scheme
- `--include`/`--exclude`: functions as you'd expect, does not weirdly expand the globs into other arguments
- `--no-force-download`: use `--force-download=false` or just ignore it
- `--no-dry-run`: use `--dry-run=false` or just ignore it
- `--max-workers` determines concurrent download threads, rather than concurrent downloaded files
- `--format`: not implemented
- `--json`: not implemented
- `--quiet`: best effort; does not guarantee exact output
- `--no-truncate`: not implemented

### Environment Variables

[`hf` documentation](https://huggingface.co/docs/huggingface_hub/en/package_reference/environment_variables)

Generic:

- [x] `HF_HOME`
- [x] `HF_HUB_CACHE`
- [x] `HF_XET_CACHE`
- [x] `HF_TOKEN`
- [x] `HF_TOKEN_PATH`
- [ ] `HF_HUB_VERBOSITY`
- [x] `HF_HUB_ETAG_TIMEOUT`
- [x] `HF_HUB_DOWNLOAD_TIMEOUT`

XET:

- [x] `HF_HUB_DISABLE_XET`
- [x] `HF_XET_CACHE`
- [ ] `HF_XET_HIGH_PERFORMANCE`
- [ ] `HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY`
- [x] `HF_XET_CHUNK_CACHE_SIZE_BYTES`
- [ ] `HF_XET_SHARD_CACHE_SIZE_LIMIT`
- [ ] `HF_XET_NUM_CONCURRENT_RANGE_GETS`

Boolean values:

- [ ] `HF_DEBUG`
- [x] `HF_HUB_OFFLINE`
- [x] `HF_HUB_DISABLE_IMPLICIT_TOKEN`
- [ ] `HF_HUB_DISABLE_PROGRESS_BARS`
- [ ] `HF_HUB_DISABLE_SYMLINKS`
- [ ] `HF_HUB_DISABLE_SYMLINKS_WARNING`
- [ ] `HF_HUB_DISABLE_EXPERIMENTAL_WARNING`
- [ ] `HF_HUB_DISABLE_TELEMETRY`
- [ ] `HF_HUB_DISABLE_UPDATE_CHECK`

From external tools:

- [ ] `DO_NOT_TRACK`
- [ ] `NO_COLOR`
- [x] `XDG_CACHE_HOME`

## Performance Tuning

### Networking

To increase bandwidth usage when your Internet connection is not saturated:

- Set multiple HF mirrors with `--hfdl-endpoint`
- Increase `--max-workers`
- Try `--hfdl-upstream-policy round-robin`
- Try switch between XET or CDN with `--hfdl-source-priority`

To limit networking usage:

- Decrease `--max-workers`
- Decrease `--hfdl-max-bandwidth`
- Set `--hfdl-api-iops` for limiting API request speed

To work on really slow networks:

- Decrease `--max-workers`
- Adjust `--hfdl-stall-timeout` and `--hfdl-stall-min-bytes`

To cooperate with specific network policy:

- Use `--hfdl-proxy` for manual proxy or direct connection
- Use `--hfdl-ipqos` for DSCP tagging (OS dependent)

### Disks

To increase disk usage, if your disk is not saturated and CPU is idling:

- Increase `--hfdl-disk-workers`

To decrease/limit disk usage:

- Set `--hfdl-disk-active`

### CPU

To decrease CPU usage:

- Limit your network speed
- Decrease `--hfdl-disk-workers`

### RAM

To limit RAM usage:

- Decrease `--hfdl-io-buffer`

## Development

Compiling:

```shell
goreleaser release --snapshot --clean
```

### OpenTelemetry

All your expected OTLP environment variables work. Example:

```shell
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
hfdl download [args...]
```

Notes:

- Set `OTEL_METRIC_EXPORT_INTERVAL` (milliseconds) for adjusting metric intervals
- Setting any of `OTEL_EXPORTER_OTLP_{,TRACES_,METRICS_,LOGS_}{TIMEOUT,CERTIFICATE,CLIENT_CERTIFICATE,CLIENT_KEY}` voids `--hfdl-proxy`/`--hfdl-ipqos` for OTLP traffic
