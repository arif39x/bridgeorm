use crate::engine;
use crate::error::{BridgeOrmError, BridgeOrmResult};
use crate::schema;
use crate::telemetry;
pub mod java;
use futures::stream::BoxStream;
use futures::StreamExt;
use once_cell::sync::Lazy;
use pyo3::exceptions::{
    PyException, PyKeyError, PyRuntimeError, PyStopAsyncIteration, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sqlx::{any::AnyRow, AnyPool, Column, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

static POOL: Lazy<std::sync::RwLock<Option<AnyPool>>> = Lazy::new(|| std::sync::RwLock::new(None));
static URL: Lazy<std::sync::RwLock<Option<String>>> = Lazy::new(|| std::sync::RwLock::new(None));

/// Converts a `BridgeOrmError` to a `PyErr`.
fn bridge_error_to_py(err: BridgeOrmError) -> PyErr {
    let err_str = err.to_string();
    match err {
        BridgeOrmError::NotFound(_, _) => PyKeyError::new_err(err_str),
        BridgeOrmError::Validation(_, _) => PyValueError::new_err(err_str),
        BridgeOrmError::Database(ref sqlx_err, _) => match sqlx_err {
            sqlx::Error::RowNotFound => PyKeyError::new_err("Resource not found"),
            _ => PyRuntimeError::new_err(err_str),
        },
        _ => PyRuntimeError::new_err(err_str),
    }
}

use crate::engine::query::QueryValue;

fn query_value_to_py(py: Python<'_>, v: QueryValue) -> PyObject {
    match v {
        QueryValue::String(s) => s.to_object(py),
        QueryValue::Int(i) => i.to_object(py),
        QueryValue::Float(f) => f.to_object(py),
        QueryValue::Bool(b) => b.to_object(py),
        QueryValue::Uuid(u) => u.to_object(py),
        QueryValue::DateTime(dt) => dt.to_object(py),
        QueryValue::Json(j) => {
            let s = j.to_string();
            let json_module = py.import_bound("json").unwrap();
            json_module.call_method1("loads", (s,)).unwrap().to_object(py)
        }
        QueryValue::Bytes(b) => b.to_object(py),
        QueryValue::Raw(raw) => {
            let dict = PyDict::new_bound(py);
            dict.set_item("sql", raw.sql).unwrap();
            let params: Vec<PyObject> = raw.params.into_iter().map(|p| query_value_to_py(py, p)).collect();
            dict.set_item("params", params).unwrap();
            dict.to_object(py)
        }
        QueryValue::Null => py.None(),
    }
}

fn py_to_query_value(py: Python<'_>, obj: &Bound<'_, PyAny>, table_name: &str, column_name: &str) -> QueryValue {
    if obj.is_none() {
        return QueryValue::Null;
    }

    let registry_guard = engine::metadata::REGISTRY.read().unwrap();
    let meta = registry_guard.mappings.get(table_name)
        .and_then(|m| m.columns.get(column_name));
    
    if let Some(m) = meta {
        match m.data_type.to_lowercase().as_str() {
            "int" | "bigint" | "integer" => {
                if let Ok(val) = obj.extract::<i64>() {
                    return QueryValue::Int(val);
                }
            }
            "bool" | "boolean" => {
                if let Ok(val) = obj.extract::<bool>() {
                    return QueryValue::Bool(val);
                }
            }
            "float" | "double precision" | "real" | "double" => {
                if let Ok(val) = obj.extract::<f64>() {
                    return QueryValue::Float(val);
                }
            }
            "uuid" => {
                if let Ok(u) = obj.extract::<uuid::Uuid>() {
                    return QueryValue::Uuid(u);
                }
                // Fallback: try parsing from string
                if let Ok(s) = obj.extract::<String>() {
                    if let Ok(u) = uuid::Uuid::parse_str(&s) {
                        return QueryValue::Uuid(u);
                    }
                }
            }
            "datetime" => {
                if let Ok(dt) = obj.extract::<chrono::DateTime<chrono::Utc>>() {
                    return QueryValue::DateTime(dt);
                }
            }
            "json" | "jsonb" => {
                 let json_module = py.import_bound("json").unwrap();
                 if let Ok(s) = json_module.call_method1("dumps", (obj,)).and_then(|r| r.extract::<String>()) {
                     if let Ok(v) = serde_json::from_str(&s) {
                         return QueryValue::Json(v);
                     }
                 }
            }
            _ => {}
        }
    }
    
    // Default heuristics if meta is missing or doesn't match
    if let Ok(b) = obj.extract::<bool>() {
        // In Python, bool is a subclass of int, so check bool first.
        // But obj.extract::<bool>() might be too lenient? 
        // Pyo3's extract::<bool> is usually strict for actual bools.
        if obj.is_instance_of::<pyo3::types::PyBool>() {
             return QueryValue::Bool(b);
        }
    }

    if let Ok(i) = obj.extract::<i64>() {
        return QueryValue::Int(i);
    }
    
    if let Ok(f) = obj.extract::<f64>() {
        return QueryValue::Float(f);
    }

    if let Ok(u) = obj.extract::<uuid::Uuid>() {
        return QueryValue::Uuid(u);
    }

    if let Ok(dt) = obj.extract::<chrono::DateTime<chrono::Utc>>() {
        return QueryValue::DateTime(dt);
    }

    if let Ok(b) = obj.extract::<Vec<u8>>() {
        return QueryValue::Bytes(b);
    }

    // Check for Raw expression
    if let Ok(sql_attr) = obj.getattr("sql") {
        if let Ok(sql) = sql_attr.extract::<String>() {
            if let Ok(params_attr) = obj.getattr("params") {
                if let Ok(params_py) = params_attr.extract::<Vec<Bound<'_, PyAny>>>() {
                    let mut params = Vec::new();
                    for p in params_py {
                        params.push(py_to_query_value(py, &p, table_name, column_name));
                    }
                    return QueryValue::Raw(crate::engine::query::RawExpression { sql, params });
                }
            }
        }
    }

    // Default to string representation
    if let Ok(val) = obj.extract::<String>() {
        QueryValue::String(val)
    } else {
        QueryValue::String(obj.to_string())
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
    #[pyo3(get)]
    pub default_value: Option<String>,
}

#[pyclass]
pub struct LazyRowStream {
    pub stream: Arc<Mutex<BoxStream<'static, BridgeOrmResult<AnyRow>>>>,
    pub table_name: String,
}

#[pymethods]
impl LazyRowStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let stream = self.stream.clone();
        let table_name = self.table_name.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = stream.lock().await;
            match stream.next().await {
                Some(Ok(row)) => Python::with_gil(|py| {
                    let dict = engine::hydrator::hydrate_row(py, &table_name, &row)?;
                    Ok(dict.to_object(py))
                }),
                Some(Err(e)) => Err(bridge_error_to_py(e)),
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

#[pyclass]
#[derive(Clone)]
pub struct TableMetaProxy {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub columns: Vec<ColumnMetaProxy>,
}

#[pyfunction]
fn reflect_schema(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let tables = schema::introspect::reflect_schema(&pool, &url)
            .await
            .map_err(bridge_error_to_py)?;

        let proxies: Vec<TableMetaProxy> = tables
            .into_iter()
            .map(|t| TableMetaProxy {
                name: t.name,
                columns: t
                    .columns
                    .into_iter()
                    .map(|c| ColumnMetaProxy {
                        name: c.name,
                        data_type: c.data_type,
                        is_nullable: c.is_nullable,
                        is_primary_key: c.is_primary_key,
                        default_value: c.default_value,
                    })
                    .collect(),
            })
            .collect();

        Ok(proxies)
    })
}

#[pyfunction]
fn reflect_table(py: Python<'_>, table_name: String) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let columns = schema::introspect::reflect_table(&pool, &url, &table_name)
            .await
            .map_err(bridge_error_to_py)?;

        let proxies: Vec<ColumnMetaProxy> = columns
            .into_iter()
            .map(|c| ColumnMetaProxy {
                name: c.name,
                data_type: c.data_type,
                is_nullable: c.is_nullable,
                is_primary_key: c.is_primary_key,
                default_value: c.default_value,
            })
            .collect();

        Ok(proxies)
    })
}

#[pyfunction]
#[pyo3(signature = (table, data, tx=None))]
fn insert_row(
    py: Python<'_>,
    table: String,
    data: Bound<'_, PyDict>,
    tx: Option<PyObject>,
) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    // Extract TxHandle or Session if provided
    let tx_mutex = if let Some(tx_obj) = tx {
        if let Ok(session) = tx_obj.extract::<engine::session::Session>(py) {
            Some(session.transaction)
        } else if let Ok(tx_handle) = tx_obj.extract::<engine::transaction::TxHandle>(py) {
            Some(tx_handle.inner)
        } else {
            return Err(PyValueError::new_err("Invalid transaction or session object"));
        }
    } else {
        None
    };

    let table_clone = table.clone();
    let mut query_data: HashMap<String, QueryValue> = HashMap::new();
    for (k, v) in data {
        let key = k.extract::<String>()?;
        query_data.insert(key.clone(), py_to_query_value(py, &v, &table_clone, &key));
    }

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = engine::db::generic_insert(
            &pool,
            tx_mutex.as_ref(),
            &url,
            &table,
            query_data,
        )
        .await
        .map_err(bridge_error_to_py)?;
        
        Python::with_gil(|py| {
            let dict = PyDict::new_bound(py);
            for (k, v) in res {
                dict.set_item(k, query_value_to_py(py, v))?;
            }
            Ok(dict.to_object(py))
        })
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters, fields=None, tx=None))]
fn find_one(
    py: Python<'_>,
    table: String,
    filters: Bound<'_, PyDict>,
    fields: Option<Vec<String>>,
    tx: Option<PyObject>,
) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    // Extract TxHandle or Session if provided
    let tx_mutex = if let Some(tx_obj) = tx {
        if let Ok(session) = tx_obj.extract::<engine::session::Session>(py) {
            Some(session.transaction)
        } else if let Ok(tx_handle) = tx_obj.extract::<engine::transaction::TxHandle>(py) {
            Some(tx_handle.inner)
        } else {
            return Err(PyValueError::new_err("Invalid transaction or session object"));
        }
    } else {
        None
    };

    let table_clone = table.clone();
    let mut query_filters: HashMap<String, QueryValue> = HashMap::new();
    for (k, v) in filters {
        let key = k.extract::<String>()?;
        query_filters.insert(key.clone(), py_to_query_value(py, &v, &table_clone, &key));
    }

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let rows = engine::db::generic_query(
            &pool,
            tx_mutex.as_ref(),
            &url,
            &table,
            query_filters,
            Some(1),
            fields,
        )
        .await
        .map_err(bridge_error_to_py)?;

        if rows.is_empty() {
            Ok(None::<PyObject>.to_object(Python::with_gil(|py| py)))
        } else {
            Python::with_gil(|py| {
                let dict = engine::hydrator::hydrate_row(py, &table, &rows[0])?;
                Ok(dict.to_object(py))
            })
        }
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters, limit=None, fields=None, tx=None))]
fn fetch_all(
    py: Python<'_>,
    table: String,
    filters: Bound<'_, PyDict>,
    limit: Option<i64>,
    fields: Option<Vec<String>>,
    tx: Option<PyObject>,
) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    // Extract TxHandle or Session if provided
    let tx_mutex = if let Some(tx_obj) = tx {
        if let Ok(session) = tx_obj.extract::<engine::session::Session>(py) {
            Some(session.transaction)
        } else if let Ok(tx_handle) = tx_obj.extract::<engine::transaction::TxHandle>(py) {
            Some(tx_handle.inner)
        } else {
            return Err(PyValueError::new_err("Invalid transaction or session object"));
        }
    } else {
        None
    };

    let table_clone = table.clone();
    let mut query_filters: HashMap<String, QueryValue> = HashMap::new();
    for (k, v) in filters {
        let key = k.extract::<String>()?;
        query_filters.insert(key.clone(), py_to_query_value(py, &v, &table_clone, &key));
    }

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let rows = engine::db::generic_query(
            &pool,
            tx_mutex.as_ref(),
            &url,
            &table,
            query_filters,
            limit,
            fields,
        )
        .await
        .map_err(bridge_error_to_py)?;
        
        Python::with_gil(|py| {
            let mut results = Vec::new();
            for row in rows {
                let dict = engine::hydrator::hydrate_row(py, &table, &row)?;
                results.push(dict.to_object(py));
            }
            Ok(results)
        })
    })
}

#[pyfunction]
#[pyo3(signature = (table, filters, limit=None, fields=None, tx=None))]
fn fetch_lazy(
    py: Python<'_>,
    table: String,
    filters: Bound<'_, PyDict>,
    limit: Option<i64>,
    fields: Option<Vec<String>>,
    tx: Option<PyObject>,
) -> PyResult<LazyRowStream> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?;

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?;

    // Extract TxHandle or Session if provided
    let tx_mutex = if let Some(tx_obj) = tx {
        if let Ok(session) = tx_obj.extract::<engine::session::Session>(py) {
            Some(session.transaction)
        } else if let Ok(tx_handle) = tx_obj.extract::<engine::transaction::TxHandle>(py) {
            Some(tx_handle.inner)
        } else {
            return Err(PyValueError::new_err("Invalid transaction or session object"));
        }
    } else {
        None
    };

    let table_clone = table.clone();
    let mut query_filters: HashMap<String, QueryValue> = HashMap::new();
    for (k, v) in filters {
        let key = k.extract::<String>()?;
        query_filters.insert(key.clone(), py_to_query_value(py, &v, &table_clone, &key));
    }

    let stream = engine::db::query_lazy(pool, tx_mutex, url, &table, query_filters, limit, fields)
        .map_err(bridge_error_to_py)?;

    Ok(LazyRowStream {
        stream: Arc::new(Mutex::new(stream)),
        table_name: table,
    })
}

#[pyfunction]
fn execute_raw(py: Python<'_>, sql: String) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        engine::db::execute_raw(&pool, &sql)
            .await
            .map_err(bridge_error_to_py)?;
        Ok(())
    })
}

#[pyfunction]
fn resolve_type(py_type: String, dialect: String) -> PyResult<String> {
    engine::db::resolve_python_type_to_sql(&py_type, &dialect).map_err(bridge_error_to_py)
}

#[pyfunction]
fn set_telemetry_logger(logger: PyObject) -> PyResult<()> {
    telemetry::logger::set_python_logger(logger);
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (table, items, tx=None))]
fn insert_rows_bulk(
    py: Python<'_>,
    table: String,
    items: Vec<Bound<'_, PyDict>>,
    tx: Option<PyObject>,
) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    // Extract TxHandle or Session if provided
    let tx_mutex = if let Some(tx_obj) = tx {
        if let Ok(session) = tx_obj.extract::<engine::session::Session>(py) {
            Some(session.transaction)
        } else if let Ok(tx_handle) = tx_obj.extract::<engine::transaction::TxHandle>(py) {
            Some(tx_handle.inner)
        } else {
            return Err(PyValueError::new_err("Invalid transaction or session object"));
        }
    } else {
        None
    };

    let table_clone = table.clone();
    let mut query_items: Vec<HashMap<String, QueryValue>> = Vec::new();
    for item in items {
        let mut query_item = HashMap::new();
        for (k, v) in item {
            let key = k.extract::<String>()?;
            query_item.insert(key.clone(), py_to_query_value(py, &v, &table_clone, &key));
        }
        query_items.push(query_item);
    }

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let res = engine::db::generic_insert_bulk(
            &pool,
            tx_mutex.as_ref(),
            &url,
            &table,
            query_items,
        )
        .await
        .map_err(bridge_error_to_py)?;
        
        Python::with_gil(|py| {
            let mut results = Vec::new();
            for item in res {
                let dict = PyDict::new_bound(py);
                for (k, v) in item {
                    dict.set_item(k, query_value_to_py(py, v))?;
                }
                results.push(dict.to_object(py));
            }
            Ok(results)
        })
    })
}

#[pyfunction]
#[pyo3(signature = (table, foreign_key, parent_id, tx=None))]
fn fetch_one_to_many(
    py: Python<'_>,
    table: String,
    foreign_key: String,
    parent_id: String,
    tx: Option<PyObject>,
) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    // Extract TxHandle or Session if provided
    let tx_mutex = if let Some(tx_obj) = tx {
        if let Ok(session) = tx_obj.extract::<engine::session::Session>(py) {
            Some(session.transaction)
        } else if let Ok(tx_handle) = tx_obj.extract::<engine::transaction::TxHandle>(py) {
            Some(tx_handle.inner)
        } else {
            return Err(PyValueError::new_err("Invalid transaction or session object"));
        }
    } else {
        None
    };

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let rows = engine::relations::fetch_one_to_many(
            &pool,
            tx_mutex.as_ref(),
            &url,
            &table,
            &foreign_key,
            &parent_id,
        )
        .await
        .map_err(bridge_error_to_py)?;

        Python::with_gil(|py| {
            let mut results = Vec::new();
            for row in rows {
                let dict = engine::hydrator::hydrate_row(py, &table, &row)?;
                results.push(dict.to_object(py));
            }
            Ok(results)
        })
    })
}

#[pyfunction]
#[pyo3(signature = (target_table, junction_table, left_key, right_key, parent_id, tx=None))]
fn fetch_many_to_many(
    py: Python<'_>,
    target_table: String,
    junction_table: String,
    left_key: String,
    right_key: String,
    parent_id: String,
    tx: Option<PyObject>,
) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    // Extract TxHandle or Session if provided
    let tx_mutex = if let Some(tx_obj) = tx {
        if let Ok(session) = tx_obj.extract::<engine::session::Session>(py) {
            Some(session.transaction)
        } else if let Ok(tx_handle) = tx_obj.extract::<engine::transaction::TxHandle>(py) {
            Some(tx_handle.inner)
        } else {
            return Err(PyValueError::new_err("Invalid transaction or session object"));
        }
    } else {
        None
    };

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let rows = engine::relations::fetch_many_to_many(
            &pool,
            tx_mutex.as_ref(),
            &url,
            &target_table,
            &junction_table,
            &left_key,
            &right_key,
            &parent_id,
        )
        .await
        .map_err(bridge_error_to_py)?;

        Python::with_gil(|py| {
            let mut results = Vec::new();
            for row in rows {
                let dict = engine::hydrator::hydrate_row(py, &target_table, &row)?;
                results.push(dict.to_object(py));
            }
            Ok(results)
        })
    })
}

#[pyfunction]
#[pyo3(signature = (table, parent_key, parent_id, tx=None))]
fn fetch_self_ref(
    py: Python<'_>,
    table: String,
    parent_key: String,
    parent_id: String,
    tx: Option<PyObject>,
) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    // Extract TxHandle or Session if provided
    let tx_mutex = if let Some(tx_obj) = tx {
        if let Ok(session) = tx_obj.extract::<engine::session::Session>(py) {
            Some(session.transaction)
        } else if let Ok(tx_handle) = tx_obj.extract::<engine::transaction::TxHandle>(py) {
            Some(tx_handle.inner)
        } else {
            return Err(PyValueError::new_err("Invalid transaction or session object"));
        }
    } else {
        None
    };

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let rows = engine::relations::fetch_self_ref(
            &pool,
            tx_mutex.as_ref(),
            &url,
            &table,
            &parent_key,
            &parent_id,
        )
        .await
        .map_err(bridge_error_to_py)?;

        Python::with_gil(|py| {
            let mut results = Vec::new();
            for row in rows {
                let dict = engine::hydrator::hydrate_row(py, &table, &row)?;
                results.push(dict.to_object(py));
            }
            Ok(results)
        })
    })
}

#[pyfunction]
fn begin_transaction(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let tx = engine::transaction::begin_transaction(&pool)
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(tx)
    })
}

#[pyfunction]
fn begin_session(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let session = engine::session::begin_session(pool)
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(session)
    })
}

#[pyfunction]
fn snapshot_entity(
    py: Python<'_>,
    session: engine::session::Session,
    key: String,
    table_name: String,
    values: Bound<'_, PyDict>,
) -> PyResult<()> {
    let mut query_values = HashMap::new();
    for (k, v) in values {
        let key = k.extract::<String>()?;
        query_values.insert(key.clone(), py_to_query_value(py, &v, &table_name, &key));
    }
    session.snapshot_entity_internal(key, table_name, query_values)
}

#[pyfunction]
fn flush(
    py: Python<'_>,
    session: engine::session::Session,
    dirty_entities: Vec<(String, String, Bound<'_, PyDict>, Bound<'_, PyDict>)>,
) -> PyResult<Bound<'_, PyAny>> {
    let pool_guard = POOL.read().unwrap();
    let pool = pool_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection pool not initialized"))?
        .clone();

    let url_guard = URL.read().unwrap();
    let url = url_guard
        .as_ref()
        .ok_or_else(|| PyException::new_err("Connection URL not initialized"))?
        .clone();

    // To make it Send-safe, we compute diffs and prepare updates synchronously (with GIL)
    // and then only pass pure Rust data into the async block.
    
    struct UpdateJob {
        table_name: String,
        diff: HashMap<String, QueryValue>,
        pk_filters: HashMap<String, QueryValue>,
        key: String,
        full_values: HashMap<String, QueryValue>,
    }
    
    let mut jobs = Vec::new();
    {
        let tracker_guard = session.dirty_tracker.lock().map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {}", e)))?;
        for (key, table_name, current_values_py, pk_filters_py) in dirty_entities {
            let mut current_values = HashMap::new();
            for (k, v) in current_values_py {
                let col_name = k.extract::<String>()?;
                current_values.insert(col_name.clone(), py_to_query_value(py, &v, &table_name, &col_name));
            }
            
            if let Some(diff) = tracker_guard.compute_diff(&key, &current_values) {
                let mut pk_filters = HashMap::new();
                for (k, v) in pk_filters_py {
                    let col_name = k.extract::<String>()?;
                    pk_filters.insert(col_name.clone(), py_to_query_value(py, &v, &table_name, &col_name));
                }
                
                jobs.push(UpdateJob {
                    table_name,
                    diff,
                    pk_filters,
                    key,
                    full_values: current_values,
                });
            }
        }
    }

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        for job in jobs {
            engine::db::generic_update(
                &pool,
                Some(&session.transaction),
                &url,
                &job.table_name,
                job.diff,
                job.pk_filters
            ).await.map_err(bridge_error_to_py)?;
            
            // Re-acquire lock to update snapshot
            let mut tracker_guard = session.dirty_tracker.lock().map_err(|e| PyRuntimeError::new_err(format!("Lock poisoned: {}", e)))?;
            tracker_guard.take_snapshot(job.key, job.table_name, job.full_values);
        }
        Ok(())
    })
}

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ColumnMetaProxy>()?;
    m.add_class::<TableMetaProxy>()?;
    m.add_class::<LazyRowStream>()?;
    m.add_class::<engine::transaction::TxHandle>()?;
    m.add_class::<engine::session::Session>()?;
    m.add_function(wrap_pyfunction!(set_telemetry_logger, m)?)?;
    m.add_function(wrap_pyfunction!(configure_logging, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(reflect_schema, m)?)?;
    m.add_function(wrap_pyfunction!(reflect_table, m)?)?;

    m.add_function(wrap_pyfunction!(begin_session, m)?)?;
    m.add_function(wrap_pyfunction!(insert_row, m)?)?;
    m.add_function(wrap_pyfunction!(insert_rows_bulk, m)?)?;
    m.add_function(wrap_pyfunction!(find_one, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_all, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_lazy, m)?)?;
    m.add_function(wrap_pyfunction!(snapshot_entity, m)?)?;
    m.add_function(wrap_pyfunction!(flush, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_one_to_many, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_many_to_many, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_self_ref, m)?)?;
    m.add_function(wrap_pyfunction!(execute_raw, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_type, m)?)?;
    m.add_function(wrap_pyfunction!(engine::metadata::register_entity, m)?)?;
    m.add_function(wrap_pyfunction!(engine::metadata::lock_registry, m)?)?;
    Ok(())
}
