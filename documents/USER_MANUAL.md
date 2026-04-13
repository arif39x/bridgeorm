# BridgeORM User Manual: The Definitive Guide

Welcome to **BridgeORM**, the high-performance, cross-language ORM that combines the safety and speed of **Rust** with the flexibility and elegance of **Python**.

---

## 1. Installation & Setup

BridgeORM is a hybrid library. To use it, you must compile the Rust core into a Python extension module.

### Prerequisites
- **Rust Toolchain**: `rustc` and `cargo` (Edition 2021)
- **Python**: 3.8 or higher
- **Maturin**: The build system for PyO3

### Building the Core
```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install build tools
pip install maturin pytest pytest-asyncio

# Build the Rust bridge in-place
maturin develop
```

---

## 2. Connecting to the Database

BridgeORM uses an asynchronous connection pool managed by the Rust core.

```python
import asyncio
from bridge_orm import connect

async def main():
    # Supports SQLite and PostgreSQL (via AnyPool)
    await connect("sqlite:production.db?mode=rwc")
    
if __name__ == "__main__":
    asyncio.run(main())
```

---

## 3. Defining Models

Models are defined using standard Python classes inheriting from `BaseModel`. You **must** provide type hints for the Migration Engine to function correctly.

```python
from bridge_orm import BaseModel
from typing import Optional
from uuid import UUID
from datetime import datetime

class User(BaseModel):
    table = "users"
    _fields = ["id", "username", "email", "age", "created_at"]

    id: str # Primary keys should currently be defined as strings
    username: str
    email: str
    age: int
    created_at: datetime
```

---

## 4. CRUD Operations

### Create
```python
# Single insert (auto-generates UUID and timestamps if missing)
user = await User.create(username="Miku", email="miku@vocaloid.jp", age=16)

# Vectorized Bulk Insert (Single FFI Crossing)
users_data = [
    {"username": f"user_{i}", "email": f"u{i}@ex.com", "age": 20}
    for i in range(1000)
]
new_users = await User.create_many(users_data)
```

### Read
```python
# Fetch by ID (consults Identity Map first)
user = await User.find_one(id="some-uuid-string")

# Fluent Querying
active_users = await User.query().filter(age=20).limit(10).fetch()

# Zero-Copy Lazy Streaming (Efficient for millions of rows)
async for user in User.query().fetch_lazy():
    print(user.username)
```

---

## 5. Transaction Management

BridgeORM provides a robust async context manager for atomic operations.

```python
from bridge_orm import transaction

async def safe_operation():
    try:
        async with transaction() as tx:
            user = await User.create(username="Admin", email="admin@root.com", tx=tx)
            # All subsequent calls in this block using 'tx' are atomic
            await user.do_something_else(tx=tx)
            
            # If an exception is raised, the entire block rolls back automatically
    except Exception as e:
        print(f"Transaction failed and rolled back: {e}")
```

---

## 6. Schema Migrations (CLI)

BridgeORM owns the database lifecycle.

### Generate a Migration
The engine diffs your current `BaseModel` definitions against a JSON snapshot.
```bash
python -m bridge_orm.cli makemigrations --name add_user_age --dialect sqlite
```

### Apply Migrations
```bash
python -m bridge_orm.cli migrate --url sqlite:production.db
```

---

## 7. Unified Observability

BridgeORM bridges Rust's high-performance telemetry into Python's native logging.

```python
import logging
from bridge_orm import configure_logging

# Configure log level and slow-query threshold (100ms)
configure_logging(level="debug", slow_query_ms=100)

# All SQL executed in Rust will now appear in your Python logs
logging.basicConfig(level=logging.DEBUG)
```

---

## 8. Security Best Practices

1. **Identifier Whitelisting**: Never pass user input directly into model `table` or `_fields` names. BridgeORM validates these against a strict regex (`^[a-zA-Z_][a-zA-Z0-9_]*$`) in Rust.
2. **Parameterized Queries**: BridgeORM uses prepared statements exclusively. You do not need to manually escape values.
3. **Data Isolation**: Always use the `transaction()` context manager for multi-table writes to prevent partial state corruption.
