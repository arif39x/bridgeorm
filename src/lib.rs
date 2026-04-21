pub mod engine;
pub mod error;
pub mod ffi;
pub mod schema;
pub mod telemetry;

pub use error::{BridgeOrmError, BridgeOrmResult};

use pyo3::prelude::*;

#[pymodule]
fn bridge_orm_rs(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    ffi::register_module(m)
}
