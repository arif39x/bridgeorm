use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct BaseModelFields {
    #[pyo3(get)]
    pub id: Uuid,
    #[pyo3(get)]
    pub created_at: DateTime<Utc>,
    #[pyo3(get)]
    pub updated_at: DateTime<Utc>,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    #[pyo3(get)]
    pub id: Uuid,
    #[pyo3(get)]
    pub username: String,
    #[pyo3(get)]
    pub email: String,
    #[pyo3(get)]
    pub created_at: DateTime<Utc>,
    #[pyo3(get)]
    pub updated_at: DateTime<Utc>,
}

#[pymethods]
impl User {
    fn __repr__(&self) -> String {
        format!(
            "User(id={}, username='{}', email='{}')",
            self.id, self.username, self.email
        )
    }
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Post {
    #[pyo3(get)]
    pub id: Uuid,
    #[pyo3(get)]
    pub title: String,
    #[pyo3(get)]
    pub user_id: Uuid,
    #[pyo3(get)]
    pub created_at: DateTime<Utc>,
    #[pyo3(get)]
    pub updated_at: DateTime<Utc>,
}

#[pymethods]
impl Post {
    fn __repr__(&self) -> String {
        format!(
            "Post(id={}, title='{}', user_id={})",
            self.id, self.title, self.user_id
        )
    }
}
