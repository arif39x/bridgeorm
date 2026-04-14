//! PyO3 FFI boundary for BridgeORM.
//! 
//! All code in this module handles conversion between Rust and Python types.
//! Rule: Isolate all PyO3 code in src/ffi/.

use pyo3::prelude::*;
use pyo3::exceptions::{PyException, PyKeyError, PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::types::PyDict;
use crate::error::{BridgeOrmError, BridgeOrmResult};
use crate::engine;
use crate::schema;
use crate::telemetry;
use sqlx::{AnyPool, Column, Row, any::AnyRow};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::StreamExt;
use futures::stream::BoxStream;

static POOL: Lazy<std::sync::RwLock<Option<AnyPool>>> = Lazy::new(|| std::sync::RwLock::new(None));
static URL: Lazy<std::sync::RwLock<Option<String>>> = Lazy::new(|| std::sync::RwLock::new(None));

/// Converts a `BridgeOrmError` to a `PyErr`.
/// Rule: All Rust errors must convert to a custom Python exception hierarchy.
fn bridge_error_to_py(err: BridgeOrmError) -> PyErr {
    match err {
        BridgeOrmError::NotFound(msg) => PyKeyError::new_err(msg),
        BridgeOrmError::Validation(msg) => PyValueError::new_err(msg),
        BridgeOrmError::Database(sqlx_err) => match sqlx_err {
            sqlx::Error::RowNotFound => PyKeyError::new_err("Resource not found"),
            _ => PyRuntimeError::new_err(sqlx_err.to_string()),
        },
        _ => PyRuntimeError::new_err(err.to_string()),
    }
}

/// Executes a closure and catches any panics, converting them to Python exceptions.
/// Rule: Never let a Rust panic propagate into Python. Use catch_unwind at every FFI entry point.
fn catch_panic<F, R>(f: F) -> PyResult<R>
where
    F: FnOnce() -> PyResult<R> + std::panic::UnwindSafe,
{
    match std::panic::catch_unwind(f) {
        Ok(result) => result,
        Err(_) => Err(PyRuntimeError::new_err("Rust panic occurred")),
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ColumnMetaProxy {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub data_type: String,
    #[pyo3(get)]
    pub is_nullable: bool,
    #[pyo3(get)]
    pub is_primary_key: bool,
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

#[pyfunction]
fn configure_logging(level: String, slow_query_ms: u64) -> PyResult<()> {
    telemetry::logger::configure_logging(&level, slow_query_ms);
    Ok(())
}

#[pyfunction]
fn connect(py: Python<'_>, url: String) -> PyResult<Bound<'_, PyAny>> {
    let url_clone = url.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let pool = engine::db::connect(&url_clone)
            .await
            .map_err(bridge_error_to_py)?;
        
        let mut p = POOL.write().unwrap();
        *p = Some(pool);
        
        let mut u = URL.write().unwrap();
        *u = Some(url_clone);
        
        Ok(())
    })
}

#[pyfunction]
fn reflect_table(py: Python<'_>, table_name: String) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    
    let url_guard = URL.read().unwrap();
    let url = url_guard.as_ref().ok_or_else(|| PyException::new_err("Connection URL not initialized"))?.clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let columns = schema::introspect::reflect_table(&pool, &url, &table_name)
            .await
            .map_err(bridge_error_to_py)?;
        
        let proxies: Vec<ColumnMetaProxy> = columns.into_iter().map(|c| ColumnMetaProxy {
            name: c.name,
            data_type: c.data_type,
            is_nullable: c.is_nullable,
            is_primary_key: c.is_primary_key,
        }).collect();
        
        Ok(proxies)
    })
}

#[pyfunction]
#[pyo3(signature = (table, data, tx=None))]
fn insert_row(py: Python<'_>, table: String, data: HashMap<String, String>, _tx: Option<PyObject>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    
    let url_guard = URL.read().unwrap();
    let url = url_guard.as_ref().ok_or_else(|| PyException::new_err("Connection URL not initialized"))?.clone();
    
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = engine::db::generic_insert(&pool, &url, &table, data)
            .await
            .map_err(bridge_error_to_py)?;
        Ok(res)
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters))]
fn find_one(py: Python<'_>, table: String, filters: HashMap<String, String>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    
    let url_guard = URL.read().unwrap();
    let url = url_guard.as_ref().ok_or_else(|| PyException::new_err("Connection URL not initialized"))?.clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = engine::db::generic_query(&pool, &url, &table, filters, Some(1))
            .await
            .map_err(bridge_error_to_py)?;
        
        if res.is_empty() {
            Ok(None.into_py(py))
        } else {
            Ok(Some(res[0].clone()).into_py(py))
        }
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters, limit=None))]
fn fetch_all(py: Python<'_>, table: String, filters: HashMap<String, String>, limit: Option<i64>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    
    let url_guard = URL.read().unwrap();
    let url = url_guard.as_ref().ok_or_else(|| PyException::new_err("Connection URL not initialized"))?.clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = engine::db::generic_query(&pool, &url, &table, filters, limit)
            .await
            .map_err(bridge_error_to_py)?;
        Ok(res)
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters, limit=None))]
fn fetch_lazy(table: String, filters: HashMap<String, String>, limit: Option<i64>) -> PyResult<LazyRowStream> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    
    let url_guard = URL.read().unwrap();
    let url = url_guard.as_ref().ok_or_else(|| PyException::new_err("Connection URL not initialized"))?;

    let stream = engine::db::query_lazy(pool, url, &table, filters, limit)
        .map_err(bridge_error_to_py)?;

    Ok(LazyRowStream {
        stream: Arc::new(Mutex::new(stream)),
    })
}

#[pyfunction]
fn execute_raw(py: Python<'_>, sql: String) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        engine::db::execute_raw(&pool, &sql)
            .await
            .map_err(bridge_error_to_py)?;
        Ok(())
    })
}

#[pyfunction]
fn resolve_type(py_type: String, dialect: String) -> PyResult<String> {
    engine::db::resolve_python_type_to_sql(&py_type, &dialect)
        .map_err(bridge_error_to_py)
}

#[pyfunction]
fn set_telemetry_logger(logger: PyObject) -> PyResult<()> {
    telemetry::logger::set_python_logger(logger);
    Ok(())
}

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ColumnMetaProxy>()?;
    m.add_class::<LazyRowStream>()?;
    m.add_function(wrap_pyfunction!(set_telemetry_logger, m)?)?;
    m.add_function(wrap_pyfunction!(configure_logging, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(insert_row, m)?)?;
    m.add_function(wrap_pyfunction!(find_one, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_all, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_lazy, m)?)?;
    m.add_function(wrap_pyfunction!(execute_raw, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_type, m)?)?;
    Ok(())
}
