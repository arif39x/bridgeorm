# Concurrency & Data Integrity

BridgeORM is designed for high-concurrency async environments. It provides several mechanisms to ensure data remains consistent across multiple tasks.

## Shared Identity Map

Unlike standard ORMs that scope the cache to a single task, BridgeORM uses a **Concurrent Shared Identity Map**.

*   **Mechanism**: A process-wide `DashMap` (Rust) that tracks `(table, primary_key)`.
*   **Benefit**: If two concurrent `asyncio.Tasks` fetch the same database row, they share the same underlying state. This prevents "Identity Fragmentation" where different parts of the application see stale or conflicting versions of the same object.

## Optimistic Concurrency Control (OCC)

To prevent the "Lost Update" problem in highly concurrent systems, BridgeORM implements mandatory version tracking.

### How it Works:
1.  Every table must include a version column (default: `_bridge_row_version`).
2.  When a row is loaded, its version is captured.
3.  On `UPDATE`, the engine emits a guarded query:
    ```sql
    UPDATE users SET name = $1, _bridge_row_version = 2
    WHERE id = $2 AND _bridge_row_version = 1;
    ```
4.  If another task updated the row in the meantime, the `affected_rows` will be `0`.
5.  BridgeORM detects this and raises a `ConcurrentUpdateError`.

## Async Runtime Safety

BridgeORM bridges the gap between **Python's Asyncio** and **Rust's Tokio**.

### Dedicated Worker Pool
All Rust/SQLx tasks are dispatched to a dedicated thread pool (`DedicatedTokioPool`). This ensures that a blocking database driver or a heavy serialization task in Rust does not "freeze" the Python event loop.

### Thread-Safe Sessions
Sessions are lightweight handles to the shared Rust engine. While you can open multiple sessions, they all benefit from the shared identity map and unified connection pooling.
