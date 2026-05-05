# Security & Standards

BridgeORM is built with a "Security-by-Design" philosophy. Every architectural decision prioritizes the safety of your data and the stability of your application.

## SQL Injection Prevention

BridgeORM is structurally immune to standard SQL injection.

*   **No String Interpolation**: The Rust engine does not accept SQL strings. It only accepts structured Query AST nodes.
*   **Mandatory Parameterisation**: Every user-provided value is bound to a placeholder ($1, $2, etc.) at the database driver level (SQLx).
*   **Identifier Validation**: Column and table names are validated against a strict regex before query construction to prevent DDL injection.

## FFI & Memory Safety

The boundary between Python and Rust is the most sensitive part of the system.

*   **Panic Recovery**: All Rust entry points are wrapped in `ffi_guard!`. This catches any unexpected Rust panics and converts them into Python `RuntimeError` exceptions, preventing the "Process Abort" that usually occurs with FFI errors.
*   **Borrow Checker Protection**: Rust's ownership model ensures that data passed to the engine is either safely cloned or exclusively owned during the duration of the query, eliminating segfaults and race conditions.

## Supply Chain Security

We rigorously audit our dependencies to prevent "Malicious Package" attacks.

*   **Cargo Deny**: Our CI pipeline uses `cargo-deny` to block any dependency that has a known CVE (vulnerability) or an incompatible license (e.g., AGPL).
*   **Minimal Defaults**: By gating heavy dependencies like Apache Arrow behind feature flags, we reduce the amount of third-party code running in your application by default.

## Telemetry & Audit

Every database operation emits a structured span via OpenTelemetry.

*   **Performance Monitoring**: Spans include duration and query type.
*   **Security Audit**: Failed queries or concurrent update collisions are logged with high-fidelity diagnostic info, allowing you to detect and investigate suspicious activity in production.
