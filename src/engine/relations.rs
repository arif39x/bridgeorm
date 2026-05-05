use crate::engine::db::{validate_identifier, SqlDialect};
use crate::error::{BridgeOrmError, BridgeOrmResult, DiagnosticInfo};
use sqlx::{any::AnyRow, AnyPool};
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn fetch_one_to_many(
    pool: &AnyPool,
    tx: Option<&Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Any>>>>>,
    url: &str,
    child_table: &str,
    foreign_key: &str,
    parent_id: &str,
) -> BridgeOrmResult<Vec<AnyRow>> {
    validate_identifier(child_table)?;
    validate_identifier(foreign_key)?;

    let dialect = SqlDialect::from_url(url).to_dialect();
    let sql = format!(
        "SELECT * FROM {} WHERE {} = {}",
        child_table,
        foreign_key,
        dialect.get_placeholder(0)
    );

    let mut query = sqlx::query(&sql).bind(parent_id);

    let rows = if let Some(tx_mutex) = tx {
        let mut tx_guard = tx_mutex.lock().await;
        let tx_conn = tx_guard.as_mut().ok_or_else(|| {
            BridgeOrmError::Validation(
                "Transaction already closed".to_string(),
                DiagnosticInfo::default(),
            )
        })?;
        query.fetch_all(&mut **tx_conn).await?
    } else {
        query.fetch_all(pool).await?
    };

    Ok(rows)
}

pub async fn fetch_many_to_many(
    pool: &AnyPool,
    tx: Option<&Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Any>>>>>,
    url: &str,
    target_table: &str,
    junction_table: &str,
    left_key: &str,
    right_key: &str,
    parent_id: &str,
) -> BridgeOrmResult<Vec<AnyRow>> {
    validate_identifier(target_table)?;
    validate_identifier(junction_table)?;
    validate_identifier(left_key)?;
    validate_identifier(right_key)?;

    let dialect = SqlDialect::from_url(url).to_dialect();
    let sql = format!(
        "SELECT t.* FROM {} t
         JOIN {} j ON t.id = j.{}
         WHERE j.{} = {}",
        target_table,
        junction_table,
        right_key,
        left_key,
        dialect.get_placeholder(0)
    );

    let mut query = sqlx::query(&sql).bind(parent_id);

    let rows = if let Some(tx_mutex) = tx {
        let mut tx_guard = tx_mutex.lock().await;
        let tx_conn = tx_guard.as_mut().ok_or_else(|| {
            BridgeOrmError::Validation(
                "Transaction already closed".to_string(),
                DiagnosticInfo::default(),
            )
        })?;
        query.fetch_all(&mut **tx_conn).await?
    } else {
        query.fetch_all(pool).await?
    };

    Ok(rows)
}

pub async fn fetch_self_ref(
    pool: &AnyPool,
    tx: Option<&Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Any>>>>>,
    url: &str,
    table: &str,
    parent_key: &str,
    parent_id: &str,
) -> BridgeOrmResult<Vec<AnyRow>> {
    validate_identifier(table)?;
    validate_identifier(parent_key)?;

    let dialect = SqlDialect::from_url(url).to_dialect();
    let sql = format!(
        "SELECT * FROM {} WHERE {} = {}",
        table,
        parent_key,
        dialect.get_placeholder(0)
    );

    let mut query = sqlx::query(&sql).bind(parent_id);

    let rows = if let Some(tx_mutex) = tx {
        let mut tx_guard = tx_mutex.lock().await;
        let tx_conn = tx_guard.as_mut().ok_or_else(|| {
            BridgeOrmError::Validation(
                "Transaction already closed".to_string(),
                DiagnosticInfo::default(),
            )
        })?;
        query.fetch_all(&mut **tx_conn).await?
    } else {
        query.fetch_all(pool).await?
    };

    Ok(rows)
}
