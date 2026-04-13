# BridgeORM Architecture, Core Concepts, and Market Comparison

## 1. The Core Concept: "The Performance Bridge"

Most ORMs are built entirely in a high-level language (Python, Ruby, JS). This creates a "Serialization Tax" where every database row must be parsed and converted into a language-specific object, often blocking the main thread.

**BridgeORM** flips this script. It moves the entire execution engine into **Rust**, using the high-performance `sqlx` driver and `tokio` runtime. Python acts only as a thin, expressive API layer.

---

## 2. Inner Architectural Logic

### A. The Non-Blocking FFI (Foreign Function Interface)
Standard FFI calls are synchronous. BridgeORM uses `pyo3-async-runtimes` to bridge the **Tokio (Rust)** and **Asyncio (Python)** event loops.
- **Logic**: When Python `awaits` a query, Rust spawns a future on the Tokio thread pool. The Python coroutine is suspended, allowing the event loop to handle other requests. When the SQL finishes, Rust signals Python to resume.

### B. Vectorized FFI Pipeline
Crossing the language boundary is expensive. 
- **The Problem**: Inserting 10,000 rows usually requires 10,000 boundary crossings.
- **The BridgeORM Solution**: The `create_many` method serializes the entire batch in Python and crosses the boundary **exactly once**. Rust then pipelines these inserts within a single transaction.

### C. Per-Task Identity Map
To prevent "split-brain" state (where two different Python objects represent the same DB row), BridgeORM implements an Identity Map using `contextvars`.
- **Logic**: The cache is scoped to the `asyncio.Task`. If you fetch User `ID: 1` twice in the same task, you get the exact same object reference. This ensures that a name change in one function is visible to another function in the same task without a DB refresh.

### D. Deterministic Migration Engine
The migration engine does not look at the database to generate migrations; it looks at the **Source of Truth** (your Python code).
- **Snapshotting**: It stores a JSON manifest of your models. 
- **Diffing**: When you run `makemigrations`, it compares the new code state against the manifest and generates the minimal SQL DDL (Data Definition Language) required.

---

## 3. Detailed Feature Set

| Feature | Description | Implementation Detail |
| :--- | :--- | :--- |
| **Async Native** | Full `async/await` support in Python and Rust. | `pyo3-async-runtimes` + `sqlx` |
| **Security Whitelist** | Prevents SQL injection via identifier manipulation. | Regex validation (`^[a-zA-Z_][a-zA-Z0-9_]*$`) in Rust. |
| **Zero-Copy Lazy Iter** | Stream millions of rows without memory bloat. | Rust-backed `AsyncIterator`. |
| **Unified Telemetry** | View Rust SQL performance in Python logs. | GIL-aware telemetry bridge. |
| **Generic Engine** | One Rust core for any number of Python models. | Metadata-driven SQL construction. |
| **Atomic Transactions** | Thread-safe, nested transaction handles. | `sqlx::Transaction` wrapped in `TxHandle`. |

---

## 4. Market Comparison

| Feature | **BridgeORM** | **SQLAlchemy (Async)** | **Django ORM** | **Prisma (Python)** |
| :--- | :--- | :--- | :--- | :--- |
| **Core Language** | Rust (High Speed) | Python (Medium) | Python (Medium) | Rust Engine (via Binary) |
| **FFI Boundary** | Optimized (Vectorized) | N/A (Pure Python) | N/A (Pure Python) | High Overhead (JSON over HTTP/Pipe) |
| **Concurrency** | Very High (Tokio) | Medium (Threaded Drivers) | Low (Synchronous Core) | High (Rust Node) |
| **Type Safety** | Strict (Rust + Hints) | Flexible | Flexible | Very Strict (Schema File) |
| **Migrations** | Deterministic (Python) | Alembic (Manual/Auto) | Integrated (Python) | Prisma Migrate (DSL) |
| **Observability** | Unified (Cross-Lang) | Python-only | Python-only | Engine-only |

### Why choose BridgeORM over SQLAlchemy?
SQLAlchemy is powerful but carries significant overhead in high-concurrency async environments. BridgeORM offloads the CPU-intensive work (SQL construction, row mapping, protocol handling) to Rust, freeing up the Python event loop for business logic.

### Why choose BridgeORM over Prisma?
Prisma uses a Rust engine, but it communicates with Python via a local HTTP server or pipe, which adds significant latency to every query. BridgeORM uses **native memory bindings (FFI)**, making data transfers significantly faster.

---

## 5. Future Roadmap (The "Production" Path)
- **N+1 Resolution (Eager Loading)**: Automatic `JOIN` generation for related models.
- **Complex Operators**: Support for `IN`, `LIKE`, `BETWEEN`, and nested `AND/OR` logic.
- **Connection Pool Tuning**: Deep integration with `sqlx` pool settings (max connections, idle timeout).
- **Auto-Reflection**: Generating Python models from existing legacy databases.
