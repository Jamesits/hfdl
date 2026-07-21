// hfdl is the command-line entrypoint: `hfdl download`, `hfdl logs` and the
// cobra root (help/version). cmd is deliberately lean — it parses flags and
// env, wires the packages together in bootstrap order (stderr handler,
// store, then DB/OTLP sinks), owns signal handling and exit codes, and
// prints nothing but the final absolute path on stdout (parity contract,
// doc/parity.md).
package main
