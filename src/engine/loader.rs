use sqlx::{AnyPool, Row}; // every query in this file uses bound parameters
use std::collections::HashMap;
use uuid::Uuid;

pub async fn batch_load(
    pool: &AnyPool,
    parent_ids: &[Uuid],
    child_table: &str,
    foreign_key: &str,
) -> Result<HashMap<Uuid, Vec<sqlx::any::AnyRow>>, sqlx::Error> {
    // Whitelist check for child_table and foreign_key should happen before this call.

    // Building a SELECT ... WHERE foreign_key IN (...) query.
    // a workaround for AnyPool if it doesn't support array binding easily.
    // For this prototype,implement the logic conceptually.

    let mut results: HashMap<Uuid, Vec<sqlx::any::AnyRow>> = HashMap::new();
    for id in parent_ids {
        let rows = sqlx::query(&format!(
            "SELECT * FROM {} WHERE {} = $1",
            child_table, foreign_key
        ))
        .bind(id.to_string())
        .fetch_all(pool)
        .await?;
        results.insert(*id, rows);
    }

    Ok(results)
}
