use pyo3::prelude::*;

#[pyclass]
#[derive(Clone, Debug)]
pub struct PoolConfig {
    #[pyo3(get, set)]
    pub max_connections: u32,
    #[pyo3(get, set)]
    pub min_connections: u32,
    #[pyo3(get, set)]
    pub connect_timeout_sec: u64,
    #[pyo3(get, set)]
    pub idle_timeout_sec: Option<u64>,
    #[pyo3(get, set)]
    pub max_lifetime_sec: Option<u64>,
}

#[pymethods]
impl PoolConfig {
    #[new]
    #[pyo3(signature = (max_connections=10, min_connections=1, connect_timeout_sec=30, idle_timeout_sec=None, max_lifetime_sec=None))]
    pub fn new(
        max_connections: u32,
        min_connections: u32,
        connect_timeout_sec: u64,
        idle_timeout_sec: Option<u64>,
        max_lifetime_sec: Option<u64>,
    ) -> Self {
        Self {
            max_connections,
            min_connections,
            connect_timeout_sec,
            idle_timeout_sec,
            max_lifetime_sec,
        }
    }
}
