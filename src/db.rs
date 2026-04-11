// every query in this file uses bound parameters.
// No string interpolation into SQL. See BRIDGEORM_SECURITY.md.
// I am Avoiding String Interpolation(currently) Cause It causes max Sql injection
//I will add it in future whhn i will ber pro in string interpolation useing in pro way

use crate::models::{Post, User};
use sqlx::{AnyPool, Row};
use std::collections::HashMap;
use uuid::Uuid;

pub async fn connect(url: &str) -> Result<AnyPool, sqlx::Error> {
    sqlx::any::install_default_drivers();
    AnyPool::connect(url).await
}

pub async fn create_user(pool: &AnyPool, username: &str, email: &str) -> Result<User, sqlx::Error> {
    let id = Uuid::new_v4();
    let user = sqlx::query_as::<_, User>(
        "INSERT INTO users (id, username, email, created_at, updated_at)
         VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         RETURNING id, username, email, created_at, updated_at",
    )
    .bind(id)
    .bind(username)
    .bind(email)
    .fetch_one(pool)
    .await?;
    Ok(user)
}

pub async fn find_user_by_id(pool: &AnyPool, id: Uuid) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn create_post(pool: &AnyPool, title: &str, user_id: Uuid) -> Result<Post, sqlx::Error> {
    let id = Uuid::new_v4();
    let post = sqlx::query_as::<_, Post>(
        "INSERT INTO posts (id, title, user_id, created_at, updated_at)
         VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         RETURNING id, title, user_id, created_at, updated_at",
    )
    .bind(id)
    .bind(title)
    .bind(user_id)
    .fetch_one(pool)
    .await?;
    Ok(post)
}

pub async fn load_related_posts(pool: &AnyPool, user_id: Uuid) -> Result<Vec<Post>, sqlx::Error> {
    sqlx::query_as::<_, Post>("SELECT * FROM posts WHERE user_id = $1")
        .bind(user_id)
        .fetch_all(pool)
        .await
}

pub async fn query_users(
    pool: &AnyPool,
    filters: HashMap<String, String>,
    limit: Option<i64>,
) -> Result<Vec<User>, sqlx::Error> {
    let mut query_str = String::from("SELECT * FROM users");
    let mut first = true;

    for (key, _) in &filters {
        if first {
            query_str.push_str(" WHERE ");
            first = false;
        } else {
            query_str.push_str(" AND ");
        }
        query_str.push_str(&format!("{} = ?", key));
    }

    if let Some(l) = limit {
        query_str.push_str(&format!(" LIMIT {}", l));
    }

    let mut query = sqlx::query_as::<_, User>(&query_str);
    for (_, val) in filters {
        query = query.bind(val);
    }

    query.fetch_all(pool).await
}
