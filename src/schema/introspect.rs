//! Introspection logic for BridgeORM.
//! 
//! This module is pure Rust and does not depend on PyO3.

use crate::error::BridgeOrmResult;
use crate::engine::db::SqlDialect;
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Row};

/// Metadata for a database column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

/// Reflects table metadata based on the database dialect.
#[must_use]
pub async fn reflect_table(
    pool: &AnyPool,
    url: &str,
    table_name: &str,
) -> BridgeOrmResult<Vec<ColumnMeta>> {
    let dialect = SqlDialect::from_url(url);
    
    match dialect {
        SqlDialect::Sqlite => reflect_sqlite(pool, table_name).await,
        _ => reflect_information_schema(pool, table_name).await,
    }
}

/// Introspection for PostgreSQL, MySQL, and MS SQL Server using standard Information Schema.
#[must_use]
async fn reflect_information_schema(
    pool: &AnyPool,
    table_name: &str,
) -> BridgeOrmResult<Vec<ColumnMeta>> {
    let rows = sqlx::query(
        "SELECT column_name, data_type, is_nullable
         FROM information_schema.columns
         WHERE table_name = $1",
    )
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let mut columns = Vec::new();
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

/// Specialized introspection for SQLite.
#[must_use]
async fn reflect_sqlite(
    pool: &AnyPool,
    table_name: &str,
) -> BridgeOrmResult<Vec<ColumnMeta>> {
    let sql = format!("PRAGMA table_info({})", table_name);
    let rows = sqlx::query(&sql).fetch_all(pool).await?;

    let mut columns = Vec::new();
    for row in rows {
        columns.push(ColumnMeta {
            name: row.get("name"),
            data_type: row.get("type"),
            is_nullable: row.get::<i32, _>("notnull") == 0,
            is_primary_key: row.get::<i32, _>("pk") == 1,
        });
    }
    Ok(columns)
}
