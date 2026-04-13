mod engine;
mod schema;
mod telemetry;

use pyo3::prelude::*;
use pyo3::exceptions::PyException;
use sqlx::AnyPool;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use engine::transaction::TxHandle;

static POOL: Lazy<std::sync::RwLock<Option<AnyPool>>> = Lazy::new(|| std::sync::RwLock::new(None));

#[pyfunction]
fn configure_logging(level: String, slow_query_ms: u64) {
    telemetry::logger::configure_logging(&level, slow_query_ms);
}

#[pyfunction]
fn begin_transaction(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let tx = pool.begin()
            .await
            .map_err(engine::db::rust_error_to_python)?;
        
        Ok(TxHandle {
            inner: std::sync::Arc::new(tokio::sync::Mutex::new(Some(tx))),
            savepoint_depth: 0,
        })
    })
}

#[pyfunction]
fn reflect_table(py: Python<'_>, table_name: String) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = schema::introspect::reflect_table(&pool, &table_name)
            .await
            .map_err(engine::db::rust_error_to_python)?;
        Ok(res)
    })
}

#[pyfunction]
fn connect(py: Python<'_>, url: String) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let pool = engine::db::connect(&url)
            .await
            .map_err(engine::db::rust_error_to_python)?;
        let mut p = POOL.write().unwrap();
        *p = Some(pool);
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (table, data, tx=None))]
fn insert_row(py: Python<'_>, table: String, data: HashMap<String, String>, tx: Option<TxHandle>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = if let Some(tx_handle) = tx {
            let mut guard = tx_handle.inner.lock().await;
            let transaction = guard.as_mut().ok_or_else(|| PyException::new_err("Transaction closed"))?;
            engine::db::generic_insert_in_tx(transaction, &table, data).await?
        } else {
            engine::db::generic_insert(&pool, &table, data).await?
        };
        Ok(res)
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters))]
fn find_one(py: Python<'_>, table: String, filters: HashMap<String, String>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = engine::db::generic_query(&pool, &table, filters, Some(1)).await?;
        if res.is_empty() {
            Ok(None)
        } else {
            Ok(Some(res[0].clone()))
        }
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters, limit=None))]
fn fetch_all(py: Python<'_>, table: String, filters: HashMap<String, String>, limit: Option<i64>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = engine::db::generic_query(&pool, &table, filters, limit).await?;
        Ok(res)
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters, limit=None))]
fn fetch_lazy(table: String, filters: HashMap<String, String>, limit: Option<i64>) -> PyResult<engine::db::LazyRowStream> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    engine::db::query_lazy(pool, &table, filters, limit)
}

#[pyfunction]
fn execute_raw(py: Python<'_>, sql: String) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        engine::db::execute_raw(&pool, &sql)
            .await
            .map_err(engine::db::rust_error_to_python)?;
        Ok(())
    })
}

#[pyfunction]
fn resolve_type(py_type: String, dialect: String) -> PyResult<String> {
    engine::db::resolve_python_type_to_sql(&py_type, &dialect)
}

#[pyfunction]
#[pyo3(signature = (table, items, tx=None))]
fn insert_rows_bulk(py: Python<'_>, table: String, items: Vec<HashMap<String, String>>, tx: Option<TxHandle>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = engine::db::generic_bulk_insert(&pool, &table, items).await?;
        Ok(res)
    })
}

#[pyfunction]
fn set_telemetry_logger(logger: PyObject) {
    telemetry::logger::set_python_logger(logger);
}

#[pymodule]
fn bridge_orm_rs(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TxHandle>()?;
    m.add_class::<engine::db::LazyRowStream>()?;
    m.add_function(wrap_pyfunction!(set_telemetry_logger, m)?)?;
    m.add_function(wrap_pyfunction!(configure_logging, m)?)?;
    m.add_function(wrap_pyfunction!(begin_transaction, m)?)?;
    m.add_function(wrap_pyfunction!(reflect_table, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(insert_row, m)?)?;
    m.add_function(wrap_pyfunction!(find_one, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_all, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_lazy, m)?)?;
    m.add_function(wrap_pyfunction!(execute_raw, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_type, m)?)?;
    m.add_function(wrap_pyfunction!(insert_rows_bulk, m)?)?;
    Ok(())
}
