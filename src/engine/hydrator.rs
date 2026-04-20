use sqlx::{any::AnyRow, Column, Row};
use std::collections::HashMap;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use crate::engine::metadata::{REGISTRY, ColumnMetadata};

pub fn hydrate_row<'py>(py: Python<'py>, table_name: &str, row: &AnyRow) -> PyResult<Bound<'py, PyDict>> {
    let registry_guard = REGISTRY.read().unwrap();
    let mapping = registry_guard.mappings.get(table_name);
    
    if mapping.is_none() {
        println!("DEBUG: No mapping found for table: {}", table_name);
        println!("DEBUG: Available mappings: {:?}", registry_guard.mappings.keys());
    }
    
    let dict = PyDict::new_bound(py);
    
    for column in row.columns() {
        let name = column.name();
        let meta = mapping.and_then(|m| m.columns.get(name));
        
        println!("DEBUG: Hydrating column: {} (has_meta: {})", name, meta.is_some());
        if let Some(m) = meta {
            println!("DEBUG:   Meta data_type: {}", m.data_type);
        }
        
        let val = if let Some(meta) = meta {
            coerce_value(py, row, name, meta)?
        } else {
            // Fallback for unmapped columns (lenient mode by default)
            let raw_val: String = row.try_get(name).unwrap_or_default();
            raw_val.to_object(py)
        };
        
        dict.set_item(name, val)?;
    }
    
    Ok(dict)
}

fn coerce_value(py: Python<'_>, row: &AnyRow, name: &str, meta: &ColumnMetadata) -> PyResult<PyObject> {
    // Basic type coercion based on metadata data_type.
    
    match meta.data_type.to_lowercase().as_str() {
        "text" | "str" | "string" => {
            if let Ok(val) = row.try_get::<String, _>(name) {
                Ok(val.to_object(py))
            } else {
                Ok(py.None())
            }
        }
        "uuid" => {
            if let Ok(val) = row.try_get::<uuid::Uuid, _>(name) {
                Ok(val.to_object(py))
            } else if let Ok(val_str) = row.try_get::<String, _>(name) {
                if let Ok(u) = uuid::Uuid::parse_str(&val_str) {
                    Ok(u.to_object(py))
                } else {
                    Ok(val_str.to_object(py))
                }
            } else {
                Ok(py.None())
            }
        }
        "datetime" | "timestamp" => {
            if let Ok(val) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(name) {
                Ok(val.to_object(py))
            } else if let Ok(val_str) = row.try_get::<String, _>(name) {
                // SQLite/other might return string
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&val_str) {
                    Ok(dt.with_timezone(&chrono::Utc).to_object(py))
                } else {
                    Ok(val_str.to_object(py))
                }
            } else {
                Ok(py.None())
            }
        }
        "int" | "bigint" | "integer" => {
            if let Ok(val) = row.try_get::<i64, _>(name) {
                 Ok(val.to_object(py))
            } else if let Ok(val_str) = row.try_get::<String, _>(name) {
                 let val: i64 = val_str.parse().unwrap_or(0);
                 Ok(val.to_object(py))
            } else {
                 Ok(py.None())
            }
        }
        "bool" | "boolean" => {
            if let Ok(val) = row.try_get::<bool, _>(name) {
                 Ok(val.to_object(py))
            } else if let Ok(val_i) = row.try_get::<i32, _>(name) {
                 Ok((val_i != 0).to_object(py))
            } else if let Ok(val_i) = row.try_get::<i64, _>(name) {
                 Ok((val_i != 0).to_object(py))
            } else if let Ok(val_s) = row.try_get::<String, _>(name) {
                 let normalized = val_s.to_lowercase();
                 let val = normalized == "true" || normalized == "1" || normalized == "t";
                 Ok(val.to_object(py))
            } else {
                 Ok(py.None())
            }
        }
        "float" | "double" | "real" => {
            if let Ok(val) = row.try_get::<f64, _>(name) {
                 Ok(val.to_object(py))
            } else if let Ok(val_str) = row.try_get::<String, _>(name) {
                 let val: f64 = val_str.parse().unwrap_or(0.0);
                 Ok(val.to_object(py))
            } else {
                 Ok(py.None())
            }
        }
        "json" | "jsonb" => {
            if let Ok(val) = row.try_get::<serde_json::Value, _>(name) {
                let s = val.to_string();
                let json_module = py.import_bound("json").unwrap();
                Ok(json_module.call_method1("loads", (s,)).unwrap().to_object(py))
            } else {
                Ok(py.None())
            }
        }
        "bytes" | "blob" | "bytea" => {
            if let Ok(val) = row.try_get::<Vec<u8>, _>(name) {
                Ok(val.to_object(py))
            } else {
                Ok(py.None())
            }
        }
        _ => {
            // Default to string representation
            if let Ok(val) = row.try_get::<String, _>(name) {
                Ok(val.to_object(py))
            } else {
                Ok(py.None())
            }
        }
    }
}
