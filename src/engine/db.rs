//! Core database engine for BridgeORM.
//! 
//! This module is pure Rust and does not depend on PyO3.
//! All functions return `BridgeOrmResult` and use `#[must_use]`.

use crate::error::{BridgeOrmError, BridgeOrmResult};
use crate::telemetry::logger::{self, TelemetryEvent};
use futures::stream::BoxStream;
use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use sqlx::{any::AnyRow, AnyPool, Row};
use std::collections::HashMap;
use std::time::Instant;

/// Constants for SQL validation and formatting.
const VALID_IDENTIFIER_REGEX: &str = r"^[a-zA-Z_][a-zA-Z0-9_]*$";
const RESERVED_KEYWORDS: [&str; 8] = [
    "SELECT", "DROP", "TABLE", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER",
];

pub static VALID_SQL_IDENTIFIER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(VALID_IDENTIFIER_REGEX).expect("Invalid hardcoded regex pattern"));

/// Represents a parameterized SQL statement.
/// Rule: All query builders must produce a (sql: &str, params: Vec<Value>) tuple.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqlStatement {
    pub sql: String,
    pub params: Vec<String>,
}

/// Represents supported SQL dialects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SqlDialect {
    Postgres,
    Sqlite,
    MySql,
    MsSql,
}

impl SqlDialect {
    /// Infers the SQL dialect from the connection URL.
    #[must_use]
    pub fn from_url(url: &str) -> Self {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Self::Postgres
        } else if url.starts_with("sqlite:") {
            Self::Sqlite
        } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Self::MySql
        } else if url.starts_with("mssql://") || url.starts_with("sqlserver://") {
            Self::MsSql
        } else {
            Self::Postgres
        }
    }

    /// Returns the appropriate placeholder for the current dialect.
    #[must_use]
    pub fn get_placeholder(&self, index: usize) -> String {
        match self {
            Self::Postgres | Self::Sqlite => format!("${}", index + 1),
            Self::MySql => "?".to_string(),
            Self::MsSql => format!("@p{}", index + 1),
        }
    }
}

/// Validates that a string is a safe SQL identifier.
/// Rule: Never silently drop errors.
#[must_use]
pub fn validate_identifier(identifier: &str) -> BridgeOrmResult<()> {
    if !VALID_SQL_IDENTIFIER_PATTERN.is_match(identifier) {
        return Err(BridgeOrmError::Validation(format!(
            "Security Violation: Invalid SQL identifier '{}'",
            identifier
        )));
    }
    
    if RESERVED_KEYWORDS.contains(&identifier.to_uppercase().as_str()) {
        return Err(BridgeOrmError::Validation(format!(
            "Security Violation: Reserved keyword '{}' used as identifier",
            identifier
        )));
    }
    Ok(())
}

/// Establishes a connection pool using the provided URL.
/// Uses sqlx's built-in pool.
#[must_use]
pub async fn connect(url: &str) -> BridgeOrmResult<AnyPool> {
    sqlx::any::install_default_drivers();
    AnyPool::connect(url).await.map_err(BridgeOrmError::from)
}

/// Shared logic for building placeholders and values for queries.
#[must_use]
fn prepare_statement(
    dialect: SqlDialect,
    data: &HashMap<String, String>,
) -> BridgeOrmResult<(Vec<String>, Vec<String>, Vec<String>)> {
    let mut columns = Vec::new();
    let mut values = Vec::new();
    let mut placeholders = Vec::new();

    for (idx, (col, val)) in data.iter().enumerate() {
        validate_identifier(col)?;
        columns.push(col.clone());
        values.push(val.clone());
        placeholders.push(dialect.get_placeholder(idx));
    }
    Ok((columns, values, placeholders))
}

/// Helper to convert a database row into a HashMap.
#[must_use]
pub fn row_to_map(row: &AnyRow) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for column in row.columns() {
        let name = column.name();
        let val: String = row.try_get(name).unwrap_or_default();
        map.insert(name.to_string(), val);
    }
    map
}

/// Pure Rust implementation of execute_raw.
#[must_use]
pub async fn execute_raw(pool: &AnyPool, sql: &str) -> BridgeOrmResult<()> {
    sqlx::query(sql).execute(pool).await?;
    Ok(())
}

/// Pure Rust generic insert.
#[must_use]
pub async fn generic_insert(
    pool: &AnyPool,
    url: &str,
    table_name: &str,
    data: HashMap<String, String>,
) -> BridgeOrmResult<HashMap<String, String>> {
    validate_identifier(table_name)?;
    let dialect = SqlDialect::from_url(url);
    let (columns, values, placeholders) = prepare_statement(dialect, &data)?;

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name,
        columns.join(", "),
        placeholders.join(", ")
    );

    let start = Instant::now();
    let mut query = sqlx::query(&sql);
    for val in &values {
        query = query.bind(val);
    }

    query.execute(pool).await?;
        
    let duration = start.elapsed();
    logger::emit_telemetry(TelemetryEvent {
        sql: sql.clone(),
        duration_micros: duration.as_micros() as u64,
        operation: "INSERT".to_string(),
        table: table_name.to_string(),
    });

    Ok(data)
}

/// Pure Rust generic query.
#[must_use]
pub async fn generic_query(
    pool: &AnyPool,
    url: &str,
    table_name: &str,
    filters: HashMap<String, String>,
    limit: Option<i64>,
) -> BridgeOrmResult<Vec<HashMap<String, String>>> {
    validate_identifier(table_name)?;
    let dialect = SqlDialect::from_url(url);
    let mut sql = format!("SELECT * FROM {}", table_name);
    let mut values = Vec::new();

    if !filters.is_empty() {
        sql.push_str(" WHERE ");
        let mut conditions = Vec::new();
        for (idx, (col, val)) in filters.iter().enumerate() {
            validate_identifier(col)?;
            conditions.push(format!("{} = {}", col, dialect.get_placeholder(idx)));
            values.push(val.clone());
        }
        sql.push_str(&conditions.join(" AND "));
    }

    if let Some(l) = limit {
        sql.push_str(&format!(" LIMIT {}", l));
    }

    let start = Instant::now();
    let mut query = sqlx::query(&sql);
    for val in &values {
        query = query.bind(val);
    }

    let rows = query.fetch_all(pool).await?;
    
    let duration = start.elapsed();
    logger::emit_telemetry(TelemetryEvent {
        sql: sql.clone(),
        duration_micros: duration.as_micros() as u64,
        operation: "SELECT".to_string(),
        table: table_name.to_string(),
    });

    Ok(rows.iter().map(row_to_map).collect())
}

/// Pure Rust implementation of lazy query.
#[must_use]
pub fn query_lazy(
    pool: &AnyPool,
    url: &str,
    table_name: &str,
    filters: HashMap<String, String>,
    limit: Option<i64>,
) -> BridgeOrmResult<BoxStream<'static, Result<AnyRow, sqlx::Error>>> {
    validate_identifier(table_name)?;
    let dialect = SqlDialect::from_url(url);
    let mut sql = format!("SELECT * FROM {}", table_name);
    let mut values = Vec::new();

    if !filters.is_empty() {
        sql.push_str(" WHERE ");
        let mut conditions = Vec::new();
        for (idx, (col, val)) in filters.iter().enumerate() {
            validate_identifier(col)?;
            conditions.push(format!("{} = {}", col, dialect.get_placeholder(idx)));
            values.push(val.clone());
        }
        sql.push_str(&conditions.join(" AND "));
    }

    if let Some(l) = limit {
        sql.push_str(&format!(" LIMIT {}", l));
    }

    let pool_clone = pool.clone();
    let stream = futures::stream::once(async move {
        let mut query = sqlx::query(&sql);
        for val in values {
            query = query.bind(val);
        }
        query.fetch_all(&pool_clone).await
    })
    .flat_map(|res| match res {
        Ok(rows) => futures::stream::iter(rows.into_iter().map(Ok)).left_stream(),
        Err(e) => futures::stream::once(async move { Err(e) }).right_stream(),
    })
    .boxed();

    Ok(stream)
}

/// Resolves a Python type name to its corresponding SQL type for a given dialect.
#[must_use]
pub fn resolve_python_type_to_sql(py_type: &str, dialect: &str) -> BridgeOrmResult<String> {
    let is_optional = py_type.starts_with("Optional[") || py_type.contains("None");
    let base_type = if is_optional {
        py_type
            .replace("Optional[", "")
            .replace("]", "")
            .replace("None", "")
            .trim()
            .to_string()
    } else {
        py_type.to_string()
    };

    let sql_type = match (base_type.as_str(), dialect.to_lowercase().as_str()) {
        ("str", _) => "TEXT".to_string(),
        ("int", d) if d.contains("postgres") => "BIGINT".to_string(),
        ("int", d) if d.contains("mysql") => "BIGINT".to_string(),
        ("int", "sqlite") => "INTEGER".to_string(),
        ("int", d) if d.contains("mssql") => "BIGINT".to_string(),
        
        ("float", d) if d.contains("postgres") => "DOUBLE PRECISION".to_string(),
        ("float", "sqlite") => "REAL".to_string(),
        ("float", d) if d.contains("mysql") => "DOUBLE".to_string(),
        
        ("bool", d) if d.contains("postgres") => "BOOLEAN".to_string(),
        ("bool", "sqlite") | ("bool", _) if dialect.contains("mysql") => "INTEGER".to_string(),
        
        ("datetime", d) if d.contains("postgres") => "TIMESTAMP WITH TIME ZONE".to_string(),
        ("datetime", _) => "TEXT".to_string(),
        
        ("UUID", _) | ("uuid", _) => {
            if dialect == "sqlite" { "TEXT".to_string() } else { "UUID".to_string() }
        }
        (t, _) if t.contains("Enum") => "TEXT".to_string(),
        (unknown, _) => {
            return Err(BridgeOrmError::Validation(format!(
                "Unsupported Python type '{}'",
                unknown
            )))
        }
    };

    if !is_optional {
        Ok(format!("{} NOT NULL", sql_type))
    } else {
        Ok(sql_type)
    }
}
