use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;
use tracing::{instrument, span, Level};

/// A globally shared, thread-safe map from (table_name, primary_key) to the
/// last-known serialised row state.
///
/// WHY: DashMap provides fine-grained shard locking so concurrent reads on
/// different rows have zero contention. A Mutex<HashMap> would serialise ALL
/// reads, which defeats the purpose of async concurrency.
pub type SharedIdentityMap = Arc<RowIdentityCache>;

#[derive(Debug, Default)]
pub struct RowIdentityCache {
    /// Key format: "{table_name}::{primary_key_value}"
    /// WHY: A composite string key avoids a nested map, keeping lookup O(1).
    row_cache: DashMap<String, CachedRowState>,
}

#[derive(Debug, Clone)]
pub struct CachedRowState {
    pub serialised_row: Value,
    /// Monotonically increasing integer. Incremented on every successful UPDATE.
    /// WHY: Version tracking is the foundation of Optimistic Concurrency Control.
    /// Without it we cannot detect when two tasks are operating on stale data.
    pub version_counter: u64,
}

impl RowIdentityCache {
    pub fn new() -> SharedIdentityMap {
        Arc::new(Self::default())
    }

    /// Returns the cache key for a specific row. Kept private so callers
    /// cannot construct keys manually and introduce inconsistency.
    fn cache_key(table_name: &str, primary_key_value: &str) -> String {
        format!("{}::{}", table_name, primary_key_value)
    }

    #[instrument(name = "identity_map.get", fields(table = %table_name, pk = %primary_key_value), skip(self))]
    pub fn get(&self, table_name: &str, primary_key_value: &str) -> Option<CachedRowState> {
        let key = Self::cache_key(table_name, primary_key_value);
        self.row_cache.get(&key).map(|entry| entry.clone())
    }

    #[instrument(name = "identity_map.insert_or_update", fields(table = %table_name, pk = %primary_key_value), skip(self, serialised_row))]
    pub fn insert_or_update(
        &self,
        table_name: &str,
        primary_key_value: &str,
        serialised_row: Value,
        version_counter: u64,
    ) {
        let key = Self::cache_key(table_name, primary_key_value);
        self.row_cache.insert(
            key,
            CachedRowState {
                serialised_row,
                version_counter,
            },
        );
    }

    #[instrument(name = "identity_map.evict", fields(table = %table_name, pk = %primary_key_value), skip(self))]
    pub fn evict(&self, table_name: &str, primary_key_value: &str) {
        let key = Self::cache_key(table_name, primary_key_value);
        self.row_cache.remove(&key);
    }
}
