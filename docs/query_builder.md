# Querying & Relations

BridgeORM provides a fluent, type-safe Query Builder that minimizes the need for raw SQL.

## Basic Querying

Queries are initiated via the `session.query()` method.

```python
results = await session.query(User) \
    .filter(status="active") \
    .limit(10) \
    .fetch()
```

### Supported Operators
The `QueryBuilder` supports standard equality filters via kwargs. For complex comparisons, use the `where` method (referencing the AST nodes).

## Eager Loading (`prefetch_related`)

To solve the **N+1 query problem**, BridgeORM implements a dual-strategy eager loader.

### 1. Joined Loading (`JOINED_FOR_TO_ONE`)
Best for `1:1` or `N:1` relationships. It emits a `LEFT JOIN` and fetches the related object in a single database round-trip.

### 2. Select-In Loading (`SELECT_IN_FOR_TO_MANY`)
Best for `1:N` or `M:N` relationships. It executes a secondary `SELECT ... WHERE pk IN (...)` query to fetch children for a whole collection of parents, avoiding Cartesian product explosions.

```python
from bridge_orm.core.query import EagerLoadingStrategy

# Fetch users and their posts in just 2 queries, regardless of user count.
users = await session.query(User) \
    .with_relation("posts", EagerLoadingStrategy.SELECT_IN_FOR_TO_MANY) \
    .fetch()
```

## Dialect Agnosticism

You never write SQL strings. The Query Builder produces a **Query AST (Abstract Syntax Tree)** that is sent to Rust.

The Rust engine uses a `Dialect` trait to ensure the correct syntax for:
*   Parameter placeholders (`$1` vs `?` vs `:1`)
*   Limit/Offset clauses
*   Common Table Expressions (CTEs)
*   Window Functions

This ensures your Python code is portable across PostgreSQL, MySQL, and SQLite.
