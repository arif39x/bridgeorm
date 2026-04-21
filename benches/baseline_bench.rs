use bridge_orm::engine::db::{connect, generic_query};
use bridge_orm::engine::metadata::register_entity;
use std::collections::HashMap;
use std::time::Instant;
use tokio::runtime::Runtime;

#[tokio::main]
async fn main() {
    let url = "sqlite::memory:";
    let pool = connect(url).await.unwrap();
    sqlx::query("CREATE TABLE users (id TEXT PRIMARY KEY, username TEXT, email TEXT, created_at TEXT, updated_at TEXT)")
        .execute(&pool)
        .await
        .unwrap();
        
    let columns = vec![
        ("id".to_string(), "str".to_string(), false, true),
        ("username".to_string(), "str".to_string(), false, false),
        ("email".to_string(), "str".to_string(), false, false),
        ("created_at".to_string(), "str".to_string(), false, false),
        ("updated_at".to_string(), "str".to_string(), false, false),
    ];
    register_entity("users".to_string(), columns).unwrap();

    let count = 10000;
    for i in 0..count {
        sqlx::query("INSERT INTO users (id, username, email, created_at, updated_at) VALUES (?, ?, ?, ?, ?)")
            .bind(format!("uuid-{}", i))
            .bind(format!("user_{}", i))
            .bind(format!("user_{}@example.com", i))
            .bind("2024-01-01T00:00:00Z")
            .bind("2024-01-01T00:00:00Z")
            .execute(&pool)
            .await
            .unwrap();
    }

    let start = Instant::now();
    let filters = HashMap::new();
    let rows = generic_query(&pool, None, url, "users", filters, Some(count as i64), None).await.unwrap();
    let duration = start.elapsed();

    println!("Fetched {} rows in Rust in {:?}", rows.len(), duration);
}
