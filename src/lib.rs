mod engine;
mod schema;
mod telemetry;
mod java_api;
mod error;
mod ffi;

pub use error::{BridgeOrmError, BridgeOrmResult};

use pyo3::prelude::*;

#[pymodule]
fn bridge_orm_rs(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    ffi::register_module(m)
}
