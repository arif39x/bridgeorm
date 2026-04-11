# BridgeORM

**BridgeORM** is a cross-language ORM (Rust+Python). It is lightweight, secure by default, and immediately familiar to developers from the **Python**, **Rust**, **Go**, and **TypeScript** ecosystems.

---

## Architecture

BridgeORM leverages the performance of Rust's `sqlx` and `tokio` while providing a fluent, idiomatic Python API via `PyO3`.

---

## 🛠 Technical Specifications

### Rust Backend (`src/`)

- **SQLx:** Used with `runtime-tokio-rustls`. Supports Postgres and SQLite.
- **Security:** Strict parameter binding only. String interpolation into SQL is forbidden.
- **FFI:** `PyO3` handles Python bindings and module initialization.
- **Logging:** `tracing` and `tracing-subscriber` for structured SQL logging.
- **Runtime:** Single global `Tokio` runtime managed via `once_cell`.

### Python Frontend (`bridge_orm/`)

- **Asyncio:** Native support for Python's async event loop.
- **Exceptions:** A named hierarchy (`BridgeORMError`, `NotFoundError`, `HookAbortedError`, etc.) that masks raw Rust errors with plain English messages.
- **Type Safety:** Full type hint support for editor autocompletion.

---

## Security Mandate

BridgeORM **Prepared Statements** for every operation. User-supplied data is never interpolated into SQL strings. Table names and savepoint names are validated against strict whitelists and regex patterns (`^[a-zA-Z_][a-zA-Z0-9_]*$`) in the Rust layer before execution.

---

## Requirements

- **Rust:** 1.70+
- **Python:** 3.8+
- **Build Tool:** `maturin`

```bash
# Compile and install locally
maturin develop
```

---

## Usage Styles

- **TypeScript/Node Feel:** Use the fluent `.query().filter().fetch()` chain.
- **Python/Django Feel:** Use class-based models and decorators for hooks.
- **Go Feel:** Explicit error handling via BridgeORM's named exception hierarchy.
- **Rust Feel:** Explicit connection management and zero "magic" hidden state.
