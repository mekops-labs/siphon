# Siphon — Agent Instructions

This file contains guidance for AI agents working in this repository.

## Project Overview

Siphon is a Go application that collects data from various sources and routes it to various destinations via an internal event bus. The core pipeline is: **Collectors → Bus → Pipelines (parse/transform/state) → Sinks**.

Module: `github.com/mekops-labs/siphon` | Go version: 1.25

## Repository Layout

```
cmd/siphon/          # Application entry point
internal/
  config/            # YAML config structs and loader
  modules/           # Auto-generated import file that registers all modules
  utils/             # Shared utilities
pkg/
  bus/               # Thread-safe pub/sub event bus
  collector/         # Collector interface + implementations (file, hass, mqtt, rest, shell, webhook)
  parser/            # Parser interface + implementations (jsonpath, regex)
  pipeline/          # Core pipeline runner (compile, transform, dispatch)
  sink/              # Sink interface + implementations (bus, gotify, hass, iotplotter, mqtt, ntfy, stdout, windy)
  editor/            # Embedded web-based config editor (ace.js)
configs/             # Example YAML configuration
tools/genmodules/    # Code generator for internal/modules/modules.go
```

## Key Interfaces

### Collector (`pkg/collector/collector.go`)
```go
type Collector interface {
    Start(b bus.Bus)
    End()
    RegisterTopic(name string, value string)
}
type Init func(params any) Collector
```

### Sink (`pkg/sink/sink.go`)
```go
type Sink interface {
    Send(b []byte) error
    Close() error  // release long-lived connections; return nil if none
}
type Init func(params any, eventBus bus.Bus) (Sink, error)
```

### Parser (`pkg/parser/parser.go`)
```go
type Parser interface {
    Parse(payload []byte, vars map[string]string) (map[string]any, error)
}
```

## Adding a New Module

### New Collector

1. Create `pkg/collector/<name>/<name>.go`.
2. Define a params struct decoded via `mapstructure`.
3. Implement `collector.Collector` (Start, End, RegisterTopic).
4. Register in `init()`:
   ```go
   func init() { collector.Registry.Add("<name>", New) }
   ```
5. Add a compile-time interface check:
   ```go
   var _ collector.Collector = (*yourType)(nil)
   ```
6. Run the module generator to update `internal/modules/modules.go`:
   ```
   go generate ./internal/modules/
   ```

### New Sink

1. Create `pkg/sink/<name>/<name>.go`.
2. Implement `sink.Sink` (Send, Close).
3. Register in `init()`:
   ```go
   func init() { sink.Registry.Add("<name>", New) }
   ```
4. `New` must return `(sink.Sink, error)` — validate required fields and return a descriptive error.
5. Run `go generate ./internal/modules/` to update the import file.

### New Parser

1. Create `pkg/parser/<name>/<name>.go`.
2. Implement `parser.Parser`.
3. Register in `init()`:
   ```go
   func init() { parser.Register("<name>", func() parser.Parser { return &yourParser{} }) }
   ```
4. Run `go generate ./internal/modules/`.

## Code Conventions

- Use `mapstructure` to decode `params any` into typed structs for all module constructors.
- Long-running goroutines must respect a `context.Context` for cancellation; use `sync.WaitGroup` and wait in `End()` / `Close()`.
- Protect shared state with `sync.Mutex`; always `defer` unlock immediately after locking.
- Log with the standard `log` package (`log.Printf`); no structured logging library.
- HTTP clients must set an explicit `Timeout` (see ntfy sink: `5 * time.Second`).
- No global mutable state beyond the module registries (which are written only during `init()`).

## Verification Commands

Run these before considering any change complete:

```sh
# Format
go fmt $(go list ./... | grep -v /vendor/)

# Vet
go vet $(go list ./... | grep -v /vendor/)

# Tests (with race detector)
go test -race ./...

# Build
go build ./...
```

CI runs the same steps (see `.github/workflows/`).

## Testing Guidelines

- Use `mockSink` / `mockParser` patterns as in `pkg/pipeline/pipeline_test.go` — define small local types that implement the interface.
- Do not mock the event bus; use `bus.NewMemoryBus()` directly.
- Tests that involve goroutines must use a `context.WithCancel` / `context.WithTimeout` to avoid leaks.

## Configuration Schema

Config files are YAML, `version: 2`. Top-level keys: `collectors`, `sinks`, `pipelines`. See `configs/example.yaml` for a runnable reference covering all major features (shell/file/rest collectors, JSONPath/regex parsers, stateful cron pipelines, multi-sink dispatch).

Environment variable substitution uses `%%VAR%%` notation. Expression evaluation uses the [expr](https://expr.medv.io/docs/Language-Definition) language.

## Build & Deployment

The project uses `ko` for container builds targeting `linux/arm`, `linux/arm64`, and `linux/amd64`. Container images are pushed to `ghcr.io/mekops-labs/siphon`. Deployment is triggered manually via `workflow_dispatch` in CI.
