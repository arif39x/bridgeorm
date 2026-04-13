# BridgeORM Security Mandates

## Parameter Binding

Every SQL statement that touches user-supplied data must use SQLx's prepared statements (e.g., `query!`, `query_as`, or `.bind()`).
String interpolation into SQL is strictly forbidden to prevent SQL injection vulnerabilities.

## Savepoint Names

All user-supplied savepoint names must be validated against the regex `^[a-zA-Z_][a-zA-Z0-9_]*$` in the Rust layer before being used in SQL queries.

## Table Names (Introspection)

During schema reflection, table and column names fetched from `information_schema` are used directly in metadata generation. Any user-supplied table names for specific reflection must be validated against the database's own schema whitelist.
