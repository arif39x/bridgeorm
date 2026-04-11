use sqlx::{AnyPool, Row};
use uuid::Uuid;

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
    parent_id: Uuid,
) -> Result<Vec<sqlx::any::AnyRow>, sqlx::Error> {
    sqlx::query(&format!(
        "SELECT * FROM {} WHERE {} = $1",
        child_table, foreign_key
    ))
    .bind(parent_id)
    .fetch_all(pool)
    .await
}

pub async fn fetch_many_to_many(
    pool: &AnyPool,
    target_table: &str,
    junction_table: &str,
    left_key: &str,
    right_key: &str,
    parent_id: Uuid,
) -> Result<Vec<sqlx::any::AnyRow>, sqlx::Error> {
    sqlx::query(&format!(
        "SELECT t.* FROM {} t
         JOIN {} j ON t.id = j.{}
         WHERE j.{} = $1",
        target_table, junction_table, right_key, left_key
    ))
    .bind(parent_id)
    .fetch_all(pool)
    .await
}

pub async fn fetch_self_ref(
    pool: &AnyPool,
    table: &str,
    parent_key: &str,
    parent_id: Uuid,
) -> Result<Vec<sqlx::any::AnyRow>, sqlx::Error> {
    sqlx::query(&format!(
        "SELECT * FROM {} WHERE {} = $1",
        table, parent_key
    ))
    .bind(parent_id)
    .fetch_all(pool)
    .await
}
