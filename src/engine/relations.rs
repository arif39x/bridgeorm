use sqlx::{AnyPool, Row, any::AnyRow};
use crate::error::BridgeOrmResult;
use crate::engine::db::validate_identifier;

pub enum RelationKind {
    OneToMany {
        foreign_key: String,
    },
    ManyToMany {
        junction_table: String,
        left_key: String,
        right_key: String,
    },
    SelfRef {
        parent_key: String,
    },
}

pub async fn fetch_one_to_many(
    pool: &AnyPool,
    child_table: &str,
    foreign_key: &str,
    parent_id: &str,
) -> BridgeOrmResult<Vec<AnyRow>> {
    validate_identifier(child_table)?;
    validate_identifier(foreign_key)?;
    
    let sql = format!(
        "SELECT * FROM {} WHERE {} = $1",
        child_table, foreign_key
    );
    
    let rows = sqlx::query(&sql)
        .bind(parent_id)
        .fetch_all(pool)
        .await?;
    
    Ok(rows)
}

pub async fn fetch_many_to_many(
    pool: &AnyPool,
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

    let sql = format!(
        "SELECT t.* FROM {} t
         JOIN {} j ON t.id = j.{}
         WHERE j.{} = $1",
        target_table, junction_table, right_key, left_key
    );

    let rows = sqlx::query(&sql)
        .bind(parent_id)
        .fetch_all(pool)
        .await?;
    
    Ok(rows)
}

pub async fn fetch_self_ref(
    pool: &AnyPool,
    table: &str,
    parent_key: &str,
    parent_id: &str,
) -> BridgeOrmResult<Vec<AnyRow>> {
    validate_identifier(table)?;
    validate_identifier(parent_key)?;

    let sql = format!(
        "SELECT * FROM {} WHERE {} = $1",
        table, parent_key
    );

    let rows = sqlx::query(&sql)
        .bind(parent_id)
        .fetch_all(pool)
        .await?;
    
    Ok(rows)
}
