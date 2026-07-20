# HFDL
You do not need to read `README.md`; it is for humans.

## Scope
High-speed Hugging Face downloader.

## Tech Stack
- Use Go for everything, and build with Goreleaser
- Use uptrace/bun for ORM

## Code Style
- Use comments to document higher-level intent; package-level comments
- Keep `cmd/*` lean, organize features into packages
- Avoid using `context.Background()`, `context.TODO()` or `nil` context in packages, use the context from caller
- Use `log/slog` for logging; always pass the logger from upstream to downstream, never use your own logger in the package; if logged arguments contain slices, wrap it with `logging.JSONValue` to keep spaces visible
- Run `go vet ./...` (must be run in `GOOS`/`GOARCH` matrix), `golangci-lint run` and `go fmt ./...` after code change

## Compilation
Always perform a full rebuild with `goreleaser build --snapshot --clean`, and use the artifacts under `dist/`. When compiling individual programs for testing, output to `out/`.
