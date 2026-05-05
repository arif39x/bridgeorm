use bridge_orm::engine::db::{connect, generic_query};
use bridge_orm::engine::metadata::register_entity;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sqlx::AnyPool;
use std::collections::HashMap;
use tokio::runtime::Runtime;

async fn setup_rows(pool: &AnyPool, count: usize) {
    for i in 0..count {
        sqlx::query("INSERT INTO users (id, username, email, created_at, updated_at) VALUES (?, ?, ?, ?, ?)")
            .bind(format!("uuid-{}", i))
            .bind(format!("user_{}", i))
            .bind(format!("user_{}@example.com", i))
            .bind("2024-01-01T00:00:00Z")
            .bind("2024-01-01T00:00:00Z")
            .execute(pool)
            .await
            .unwrap();
    }
}

fn bench_rust_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let url = "sqlite::memory:";

    rt.block_on(async {
        let pool = connect(url).await.unwrap();
        sqlx::query("CREATE TABLE users (id TEXT PRIMARY KEY, username TEXT, email TEXT, created_at TEXT, updated_at TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        // Register entity
        let columns = vec![
            ("id".to_string(), "str".to_string(), false, true),
            ("username".to_string(), "str".to_string(), false, false),
            ("email".to_string(), "str".to_string(), false, false),
            ("created_at".to_string(), "str".to_string(), false, false),
            ("updated_at".to_string(), "str".to_string(), false, false),
        ];
        register_entity("users".to_string(), columns).unwrap();

        // 1000 rows
        setup_rows(&pool, 1000).await;

        let pool_1000 = pool.clone();
        c.bench_function("rust_fetch_1000", |b| {
            b.to_async(&rt).iter(|| async {
                let filters = HashMap::new();
                let rows = generic_query(&pool_1000, None, url, "users", filters, Some(1000), None).await.unwrap();
                black_box(rows);
            });
        });

        // 10000 rows
        sqlx::query("DELETE FROM users").execute(&pool).await.unwrap();
        setup_rows(&pool, 10000).await;

        let pool_10000 = pool.clone();
        c.bench_function("rust_fetch_10000", |b| {
            b.to_async(&rt).iter(|| async {
                let filters = HashMap::new();
                let rows = generic_query(&pool_10000, None, url, "users", filters, Some(10000), None).await.unwrap();
                black_box(rows);
            });
        });

        // Benchmark Arrow marshalling specifically
        let rows = generic_query(&pool, None, url, "users", HashMap::new(), Some(10000), None).await.unwrap();
        c.bench_function("arrow_marshal_10000", |b| {
            b.iter(|| {
                let bytes = bridge_orm::engine::arrow::rows_to_arrow_ipc("users", &rows).unwrap();
                black_box(bytes);
            });
        });
    });
}

criterion_group!(benches, bench_rust_query);
criterion_main!(benches);
