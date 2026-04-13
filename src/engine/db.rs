// every query in this file uses bound parameters.
use crate::telemetry::logger::{self, TelemetryEvent};
use futures::stream::BoxStream;
use futures::StreamExt;
use once_cell::sync::Lazy;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use regex::Regex;
use sqlx::{any::AnyRow, AnyPool, Column, Row};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

pub static VALID_SQL_IDENTIFIER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap());

pub fn validate_identifier(id: &str) -> PyResult<()> {
    if !VALID_SQL_IDENTIFIER_PATTERN.is_match(id) {
        return Err(PyValueError::new_err(format!(
            "Security Violation: Invalid SQL identifier '{}'",
            id
        )));
    }
    let reserved = [
        "SELECT", "DROP", "TABLE", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER",
    ];
    if reserved.contains(&id.to_uppercase().as_str()) {
        return Err(PyValueError::new_err(format!(
            "Security Violation: Reserved keyword '{}' used as identifier",
            id
        )));
    }
    Ok(())
}

pub async fn connect(url: &str) -> Result<AnyPool, sqlx::Error> {
    sqlx::any::install_default_drivers();
    AnyPool::connect(url).await
}

pub async fn execute_raw(pool: &AnyPool, sql: &str) -> Result<(), sqlx::Error> {
    sqlx::query(sql).execute(pool).await?;
    Ok(())
}

pub async fn generic_insert(
    pool: &AnyPool,
    table: &str,
    data: HashMap<String, String>,
) -> PyResult<HashMap<String, String>> {
    validate_identifier(table)?;
    let mut columns = Vec::new();
    let mut values = Vec::new();
    let mut place_holders = Vec::new();

    for (idx, (col, val)) in data.into_iter().enumerate() {
        validate_identifier(&col)?;
        columns.push(col);
        values.push(val);
        place_holders.push(format!("${}", idx + 1));
    }

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
        table,
        columns.join(", "),
        place_holders.join(", ")
    );

    let start = Instant::now();
    let row = sqlx::query(&sql);
    let mut query = row;
    for val in &values {
        query = query.bind(val);
    }

    let res_row = query
        .fetch_one(pool)
        .await
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let duration = start.elapsed();

    logger::emit_telemetry(TelemetryEvent {
        sql: sql.clone(),
        duration_micros: duration.as_micros() as u64,
        operation: "INSERT".to_string(),
        table: table.to_string(),
    });

    let mut res = HashMap::new();
    for column in res_row.columns() {
        let name = column.name();
        let val: String = res_row.try_get(name).unwrap_or_default();
        res.insert(name.to_string(), val);
    }
    Ok(res)
}

pub async fn generic_insert_in_tx(
    tx: &mut sqlx::Transaction<'static, sqlx::Any>,
    table: &str,
    data: HashMap<String, String>,
) -> PyResult<HashMap<String, String>> {
    validate_identifier(table)?;
    let mut columns = Vec::new();
    let mut values = Vec::new();
    let mut place_holders = Vec::new();

    for (idx, (col, val)) in data.into_iter().enumerate() {
        validate_identifier(&col)?;
        columns.push(col);
        values.push(val);
        place_holders.push(format!("${}", idx + 1));
    }

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
        table,
        columns.join(", "),
        place_holders.join(", ")
    );

    let start = Instant::now();
    let mut query = sqlx::query(&sql);
    for val in &values {
        query = query.bind(val);
    }

    let res_row = query
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let duration = start.elapsed();

    logger::emit_telemetry(TelemetryEvent {
        sql: sql.clone(),
        duration_micros: duration.as_micros() as u64,
        operation: "INSERT_TX".to_string(),
        table: table.to_string(),
    });

    let mut res = HashMap::new();
    for column in res_row.columns() {
        let name = column.name();
        let val: String = res_row.try_get(name).unwrap_or_default();
        res.insert(name.to_string(), val);
    }
    Ok(res)
}

pub async fn generic_bulk_insert(
    pool: &AnyPool,
    table: &str,
    items: Vec<HashMap<String, String>>,
) -> PyResult<Vec<HashMap<String, String>>> {
    validate_identifier(table)?;
    if items.is_empty() {
        return Ok(Vec::new());
    }

    let start = Instant::now();
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let mut results = Vec::new();

    for data in items {
        let mut columns = Vec::new();
        let mut values = Vec::new();
        let mut place_holders = Vec::new();

        for (idx, (col, val)) in data.into_iter().enumerate() {
            validate_identifier(&col)?;
            columns.push(col);
            values.push(val);
            place_holders.push(format!("${}", idx + 1));
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
            table,
            columns.join(", "),
            place_holders.join(", ")
        );

        let mut query = sqlx::query(&sql);
        for val in values {
            query = query.bind(val);
        }

        let row = query
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut res = HashMap::new();
        for column in row.columns() {
            let name = column.name();
            let val: String = row.try_get(name).unwrap_or_default();
            res.insert(name.to_string(), val);
        }
        results.push(res);
    }

    tx.commit()
        .await
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let duration = start.elapsed();

    logger::emit_telemetry(TelemetryEvent {
        sql: format!("INSERT BULK {} rows", results.len()),
        duration_micros: duration.as_micros() as u64,
        operation: "BULK_INSERT".to_string(),
        table: table.to_string(),
    });

    Ok(results)
}

pub async fn generic_query(
    pool: &AnyPool,
    table: &str,
    filters: HashMap<String, String>,
    limit: Option<i64>,
) -> PyResult<Vec<HashMap<String, String>>> {
    validate_identifier(table)?;
    let mut sql = format!("SELECT * FROM {}", table);
    let mut first = true;
    let mut values = Vec::new();

    for (idx, (col, val)) in filters.into_iter().enumerate() {
        validate_identifier(&col)?;
        if first {
            sql.push_str(" WHERE ");
            first = false;
        } else {
            sql.push_str(" AND ");
        }
        sql.push_str(&format!("{} = ${}", col, idx + 1));
        values.push(val);
    }

    if let Some(l) = limit {
        sql.push_str(&format!(" LIMIT {}", l));
    }

    let start = Instant::now();
    let mut query = sqlx::query(&sql);
    for val in &values {
        query = query.bind(val);
    }

    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let duration = start.elapsed();

    logger::emit_telemetry(TelemetryEvent {
        sql: sql.clone(),
        duration_micros: duration.as_micros() as u64,
        operation: "SELECT".to_string(),
        table: table.to_string(),
    });

    let mut results = Vec::new();
    for row in rows {
        let mut map = HashMap::new();
        for column in row.columns() {
            let name = column.name();
            let val: String = row.try_get(name).unwrap_or_default();
            map.insert(name.to_string(), val);
        }
        results.push(map);
    }
    Ok(results)
}

#[pyclass]
pub struct LazyRowStream {
    pub stream: Arc<Mutex<BoxStream<'static, Result<AnyRow, sqlx::Error>>>>,
}

#[pymethods]
impl LazyRowStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = stream.lock().await;
            match stream.next().await {
                Some(Ok(row)) => Python::with_gil(|py| {
                    let dict = PyDict::new_bound(py);
                    for column in row.columns() {
                        let name = column.name();
                        let val: String = row.try_get(name).unwrap_or_default();
                        dict.set_item(name, val)?;
                    }
                    Ok(dict.to_object(py))
                }),
                Some(Err(e)) => Err(PyRuntimeError::new_err(e.to_string())),
                None => Err(PyStopAsyncIteration::new_err("Stream exhausted")),
            }
        })
    }
}

pub fn query_lazy(
    pool: &AnyPool,
    table: &str,
    filters: HashMap<String, String>,
    limit: Option<i64>,
) -> PyResult<LazyRowStream> {
    validate_identifier(table)?;
    let mut sql = format!("SELECT * FROM {}", table);
    let mut first = true;
    let mut values = Vec::new();

    for (idx, (col, val)) in filters.clone().into_iter().enumerate() {
        validate_identifier(&col)?;
        if first {
            sql.push_str(" WHERE ");
            first = false;
        } else {
            sql.push_str(" AND ");
        }
        sql.push_str(&format!("{} = ${}", col, idx + 1));
        values.push(val);
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

    Ok(LazyRowStream {
        stream: Arc::new(Mutex::new(stream)),
    })
}

/// Resolves a Python type name to its corresponding SQL type for a given dialect.
pub fn resolve_python_type_to_sql(py_type: &str, dialect: &str) -> PyResult<String> {
    let is_optional = py_type.starts_with("Optional[") || py_type.contains("None");
    let base_type = if is_optional {
        // Simple extraction for prototype: Optional[int] -> int
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
        ("int", "postgres") | ("int", "postgresql") => "BIGINT".to_string(),
        ("int", "sqlite") => "INTEGER".to_string(),
        ("float", "postgres") | ("float", "postgresql") => "DOUBLE PRECISION".to_string(),
        ("float", "sqlite") => "REAL".to_string(),
        ("bool", "postgres") | ("bool", "postgresql") => "BOOLEAN".to_string(),
        ("bool", "sqlite") => "INTEGER".to_string(), // 0 or 1
        ("datetime", "postgres") | ("datetime", "postgresql") => {
            "TIMESTAMP WITH TIME ZONE".to_string()
        }
        ("datetime", "sqlite") => "TEXT".to_string(), // ISO 8601
        ("date", "postgres") | ("date", "postgresql") => "DATE".to_string(),
        ("date", "sqlite") => "TEXT".to_string(),
        ("UUID", _) | ("uuid", _) => {
            if dialect == "sqlite" {
                "TEXT".to_string()
            } else {
                "UUID".to_string()
            }
        }
        // Enum support would require inspecting the Enum values,
        // for now we default to TEXT with a conceptual CHECK constraint.
        (t, _) if t.contains("Enum") => "TEXT".to_string(),
        (unknown, _) => {
            return Err(PyValueError::new_err(format!(
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

pub fn rust_error_to_python(err: sqlx::Error) -> PyErr {
    match err {
        sqlx::Error::RowNotFound => PyKeyError::new_err("Resource not found"),
        sqlx::Error::Database(e) if e.is_unique_violation() => {
            PyValueError::new_err("Database constraint violation")
        }
        _ => PyRuntimeError::new_err(format!("Database error: {}", err)),
    }
}
