# Architecture Overview

BridgeORM follows a **Hybrid Native** architecture. It is split into two distinct layers that communicate over a high-speed FFI (Foreign Function Interface) boundary using PyO3.

## The Layered Split

### 1. Python Expression Layer (`bridge_orm/`)
*   **Responsibility**: User-facing API, Model definitions, Query construction (AST building), and Session management.
*   **Key Components**:
    *   `BaseModel`: Declarative schema definition using Python types.
    *   `QueryBuilder`: A fluent DSL that builds a serializable Query AST.
    *   `Session`: The "Unit of Work" that tracks object lifecycle and provides a bridge to the Rust engine.

### 2. Rust Execution Engine (`src/`)
*   **Responsibility**: SQL Compilation, Connection Pooling (via SQLx), Data Hydration, and Telemetry emission.
*   **Key Components**:
    *   `SqlCompiler`: Translates Python's Query AST into dialect-specific parameterised SQL.
    *   `SharedIdentityMap`: A thread-safe cache backed by `DashMap` for deduplicating database rows.
    *   `FFI Module`: Exposes native functions to Python and handles type coercion.

## The "Performance Bridge" Pattern

BridgeORM avoids the common pitfalls of hybrid systems by ensuring that:
1.  **Work stays in the right place**: Complex business logic remains in Python, while heavy computation (SQL generation, result set parsing) is offloaded to Rust.
2.  **GIL Awareness**: The Python Global Interpreter Lock (GIL) is released immediately upon entering the Rust engine for any I/O-bound or heavy CPU-bound task.
3.  **Dedicated Runtimes**: Rust operations run on a process-wide `DedicatedTokioPool` to prevent interference between Python's `asyncio` event loop and Rust's internal async tasks.

## FFI Boundary Safety

Any new feature crossing the Python/Rust boundary **MUST** be wrapped in the `ffi_guard!` macro.

```rust
#[pyfunction]
fn fetch_data(py: Python<'_>) -> PyResult<PyObject> {
    ffi_guard!(py, async move {
        // Rust work happens here...
        // Any panic is caught and converted to a Python RuntimeError.
    })
}
```

This ensures that internal Rust errors do not abort the Python process, maintaining high application availability.
