# HFDL
You do not need to read `README.md`; it is for humans.

## Scope
High-speed Hugging Face downloader.

## Tech Stack
- Use Go for everything, and build with Goreleaser
- Use uptrace/bun for ORM

## Code Style
- Use comments only to document design intentions, common pits and falls, incompatiblities and cross-platform behaviour differences; package-level comments should go into a separate `package.go` file
- Keep different logical segments of the same package in different files
- Avoid using `context.Background()`, `context.TODO()` or `nil` context in packages, use the context from caller
- Use `log/slog` for logging; if logged arguments contain slices, wrap it with `logging.JSONValue` to keep spaces visible
- Common hookable types e.g. logger, context and `http.Client` must always be passed from upstream to downstream; downstream packages should never create internal ones
- Keep `cmd/*` lean, organize features into packages

## Development
Always perform a full rebuild with `goreleaser build --snapshot --clean`, and use the artifacts under `dist/`. When compiling individual programs for testing, output to `out/`. Run `go vet ./...` (must be run in `GOOS`/`GOARCH` matrix), `golangci-lint run` and `go fmt ./...` after code change.
