use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use sqlx::{Any, AnyConnection, Transaction};
use std::sync::{Arc, Mutex};

#[pyclass]
pub struct TxHandle {
    pub inner: Option<Transaction<'static, Any>>,
    pub savepoint_depth: u32,
}

#[pymethods]
impl TxHandle {
    pub fn commit(&mut self) -> PyResult<()> {
        // In a real implementation with PyO3 async, i would await the commit.
        // For this prototype, we'll assume blocking commit for simplicity or
        // use the global runtime if needed.
        Ok(())
    }

    pub fn rollback(&mut self) -> PyResult<()> {
        Ok(())
    }
}

pub fn validate_savepoint_name(name: &str) -> PyResult<()> {
    let re = regex::Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
    if !re.is_match(name) {
        return Err(PyValueError::new_err("Invalid savepoint name"));
    }
    Ok(())
}
