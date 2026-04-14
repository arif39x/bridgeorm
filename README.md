# BridgeORM

**BridgeORM** is a cross-language ORM (Rust+Python). It is lightweight, secure by default, and immediately familiar to developers from the **Python**, **Rust**, **Go**, and **TypeScript** ecosystems.

---

## Architecture

BridgeORM leverages the performance of Rust's `sqlx` and `tokio` while providing a fluent, idiomatic Python API via `PyO3`.

---

## Technical Specifications

### Rust Backend (`src/`)

- **SQLx:** Used with `runtime-tokio-rustls`.
- **Supported Databases:**
    - **PostgreSQL**: Native support.
    - **SQLite**: Native support.
    - **MySQL / MariaDB**: Native support (added).
    - **MS SQL Server (MSSQL)**: Native support (added).
    - **Oracle DB**: Support planned for future releases.
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

## Coding Rules (For Contributors)

If you want to contribute, please follow these 8 rules properly:

1. **Self-Documenting Code**: Meaningful identifiers Must. If something doesn't make sense to you; then rename it to something appropriate so that its logic will inspire clarity.
2. **Single Responsibility Principle**: Each method must have only one responsibility and be of equal simplicity.
3. **D.R.Y. Principle**: Do not duplicate common functionality; instead; utilise a single reference point when using common functionality.
4. **Meaningful Identifier**: Write your identifiers as if they were spoken words. Use common sense when naming them; avoid unnecessary jargon names; and choose names with the focus of clarity.
5. **Avoid Magic Number/Strings**: Use variable constants for hard-coded numbers/strings, so their meaning is clear.
6. **Explicit Handling of Errors**: Fix the actual problem first (i.e., fix the code), then use either typed return value or exception handling to guarantee that the error is visible and cannot go unaddressed.
7. **Consistent Formatting**: Use automated tools for visual consistency across the entire codebase (if possible).
8. **Provide Explanation to your Intent**: Comment on your code to explain "why" you made those coding decisions versus only providing commentary on "what" the code is doing.

---

## Requirements

- **Rust:** 1.70+
- **Python:** 3.8+
- **Java:** JDK 11+ (for JNI)
- **Kotlin:** 1.8+
- **Build Tool:** `maturin` (for Python), `cargo` (for JNI)

```bash
# Python: Compile and install locally
maturin develop

# Java/Kotlin: Compile the JNI library
cargo build --release
# Copy target/release/libbridge_orm_rs.so (Linux) or .dll (Windows) to your Java project
```

---

## Usage Styles

- **Java/Spring Feel:** Use repository patterns and explicit dependency injection.
- **Kotlin/Exposed Feel:** Use a statically typed DSL for querying.
- **TypeScript/Node Feel:** Use the fluent `.query().filter().fetch()` chain.
- **Python/Django Feel:** Use class-based models and decorators for hooks.
- **Go Feel:** Explicit error handling via BridgeORM's named exception hierarchy.
- **Rust Feel:** Explicit connection management and zero "magic" hidden state.
named exception hierarchy.
- **Rust Feel:** Explicit connection management and zero "magic" hidden state.
