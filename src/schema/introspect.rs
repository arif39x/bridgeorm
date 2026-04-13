// every query in this file uses bound parameters.
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Column, Row};

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub data_type: String,
    #[pyo3(get)]
    pub is_nullable: bool,
    #[pyo3(get)]
    pub is_primary_key: bool,
}

pub async fn reflect_table(
    pool: &AnyPool,
    table_name: &str,
) -> Result<Vec<ColumnMeta>, sqlx::Error> {
    // For SQLite, information_schema doesn't exist. use PRAGMA table_info(table_name).
    let mut columns = Vec::new();

    // Whitelist check would go here in a production system.

    let rows = sqlx::query(
        "SELECT column_name, data_type, is_nullable
         FROM information_schema.columns
         WHERE table_name = $1",
    )
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    for row in rows {
        columns.push(ColumnMeta {
            name: row.get("column_name"),
            data_type: row.get("data_type"),
            is_nullable: row.get::<String, _>("is_nullable") == "YES",
            is_primary_key: false, // Simplified for prototype
        });
    }

    Ok(columns)
}
