use sqlx::AnyPool;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct PoolManager {
    pools: Arc<RwLock<HashMap<String, AnyPool>>>,
    urls: Arc<RwLock<HashMap<String, String>>>,
    default_key: Arc<RwLock<Option<String>>>,
}

impl PoolManager {
    pub fn new() -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            urls: Arc::new(RwLock::new(HashMap::new())),
            default_key: Arc::new(RwLock::new(None)),
        }
    }

    pub fn register(&self, key: String, pool: AnyPool, url: String) {
        let mut pools = self.pools.write().unwrap();
        pools.insert(key.clone(), pool);
        let mut urls = self.urls.write().unwrap();
        urls.insert(key, url);
    }

    pub fn get(&self, key: Option<&str>) -> Option<(AnyPool, String)> {
        let pools = self.pools.read().unwrap();
        let urls = self.urls.read().unwrap();
        let actual_key = match key {
            Some(k) => k.to_string(),
            None => self.default_key.read().unwrap().clone()?,
        };
        Some((pools.get(&actual_key)?.clone(), urls.get(&actual_key)?.clone()))
    }

    pub fn set_default(&self, key: String) {
        let mut default = self.default_key.write().unwrap();
        *default = Some(key);
    }

    pub fn get_default_key(&self) -> Option<String> {
        self.default_key.read().unwrap().clone()
    }

    pub fn remove(&self, key: &str) {
        self.pools.write().unwrap().remove(key);
        self.urls.write().unwrap().remove(key);
        let mut default = self.default_key.write().unwrap();
        if default.as_deref() == Some(key) {
            *default = None;
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        self.pools.read().unwrap().contains_key(key)
    }
}

static POOL_MANAGER: once_cell::sync::Lazy<PoolManager> =
    once_cell::sync::Lazy::new(PoolManager::new);

pub fn pool_manager() -> &'static PoolManager {
    &POOL_MANAGER
}
