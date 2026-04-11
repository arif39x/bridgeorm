mod db;
mod models;
mod logger;
mod transaction;
mod introspect;
mod loader;
mod relations;

use pyo3::prelude::*;
use pyo3::exceptions::PyException;
use sqlx::{AnyPool, Error as SqlxError};
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use uuid::Uuid;
use transaction::TxHandle;
use introspect::ColumnMeta;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));
static POOL: Lazy<std::sync::RwLock<Option<AnyPool>>> = Lazy::new(|| std::sync::RwLock::new(None));

#[pyfunction]
fn configure_logging(level: String, slow_query_ms: u64) {
    logger::configure_logging(&level, slow_query_ms);
}

#[pyfunction]
fn begin_transaction() -> PyResult<TxHandle> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    
    let tx = RUNTIME.block_on(pool.begin())
        .map_err(rust_error_to_python)?;
    
    Ok(TxHandle {
        inner: Some(tx),
        savepoint_depth: 0,
    })
}

#[pyfunction]
fn reflect_table(table_name: String) -> PyResult<Vec<ColumnMeta>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    
    RUNTIME.block_on(introspect::reflect_table(pool, &table_name))
        .map_err(rust_error_to_python)
}

#[pyfunction]
fn connect(url: String) -> PyResult<()> {
    let pool = RUNTIME.block_on(db::connect(&url))
        .map_err(rust_error_to_python)?;
    let mut p = POOL.write().unwrap();
    *p = Some(pool);
    Ok(())
}

#[pyfunction]
fn create_user(username: String, email: String) -> PyResult<models::User> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    RUNTIME.block_on(db::create_user(pool, &username, &email))
        .map_err(rust_error_to_python)
}

#[pyfunction]
fn find_user_by_id(id: String) -> PyResult<Option<models::User>> {
    let uuid = Uuid::parse_str(&id).map_err(|_| PyException::new_err("Invalid UUID format"))?;
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    RUNTIME.block_on(db::find_user_by_id(pool, uuid))
        .map_err(rust_error_to_python)
}

#[pyfunction]
fn create_post(title: String, user_id: String) -> PyResult<models::Post> {
    let uuid = Uuid::parse_str(&user_id).map_err(|_| PyException::new_err("Invalid UUID format"))?;
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    RUNTIME.block_on(db::create_post(pool, &title, uuid))
        .map_err(rust_error_to_python)
}

#[pyfunction]
fn load_related_posts(user_id: String) -> PyResult<Vec<models::Post>> {
    let uuid = Uuid::parse_str(&user_id).map_err(|_| PyException::new_err("Invalid UUID format"))?;
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    RUNTIME.block_on(db::load_related_posts(pool, uuid))
        .map_err(rust_error_to_python)
}

#[pyfunction]
fn query_users(filters: HashMap<String, String>, limit: Option<i64>) -> PyResult<Vec<models::User>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard.as_ref().ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;
    RUNTIME.block_on(db::query_users(pool, filters, limit))
        .map_err(rust_error_to_python)
}

fn rust_error_to_python(err: SqlxError) -> PyErr {
    match err {
        SqlxError::RowNotFound => PyErr::new::<pyo3::exceptions::PyKeyError, _>("Resource not found"),
        SqlxError::Database(e) if e.is_unique_violation() => PyErr::new::<pyo3::exceptions::PyValueError, _>("Database constraint violation"),
        _ => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Database error: {}", err)),
    }
}

#[pymodule]
fn bridge_orm_rs(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TxHandle>()?;
    m.add_class::<ColumnMeta>()?;
    m.add_function(wrap_pyfunction!(configure_logging, m)?)?;
    m.add_function(wrap_pyfunction!(begin_transaction, m)?)?;
    m.add_function(wrap_pyfunction!(reflect_table, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(create_user, m)?)?;
    m.add_function(wrap_pyfunction!(find_user_by_id, m)?)?;
    m.add_function(wrap_pyfunction!(create_post, m)?)?;
    m.add_function(wrap_pyfunction!(load_related_posts, m)?)?;
    m.add_function(wrap_pyfunction!(query_users, m)?)?;
    m.add_class::<models::User>()?;
    m.add_class::<models::Post>()?;
    Ok(())
}
