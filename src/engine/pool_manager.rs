use crate::error::{BridgeError, BridgeResult, DiagnosticInfo};
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

    fn poison_err(lock_name: &str) -> BridgeError {
        BridgeError::Internal(
            format!("Pool manager lock poisoned: {}", lock_name),
            DiagnosticInfo::default(),
        )
    }

    pub fn register(&self, key: String, pool: AnyPool, url: String) -> BridgeResult<()> {
        let mut pools = self.pools.write().map_err(|_| Self::poison_err("pools"))?;
        pools.insert(key.clone(), pool);
        let mut urls = self.urls.write().map_err(|_| Self::poison_err("urls"))?;
        urls.insert(key, url);
        Ok(())
    }

    pub fn get(&self, key: Option<&str>) -> BridgeResult<Option<(AnyPool, String)>> {
        let pools = self.pools.read().map_err(|_| Self::poison_err("pools"))?;
        let urls = self.urls.read().map_err(|_| Self::poison_err("urls"))?;
        let actual_key = match key {
            Some(k) => k.to_string(),
            None => match self.default_key.read().map_err(|_| Self::poison_err("default_key"))?.clone() {
                Some(k) => k,
                None => return Ok(None),
            },
        };
        match (pools.get(&actual_key), urls.get(&actual_key)) {
            (Some(pool), Some(url)) => Ok(Some((pool.clone(), url.clone()))),
            _ => Ok(None),
        }
    }

    pub fn set_default(&self, key: String) -> BridgeResult<()> {
        let mut default = self.default_key.write().map_err(|_| Self::poison_err("default_key"))?;
        *default = Some(key);
        Ok(())
    }

    pub fn get_default_key(&self) -> BridgeResult<Option<String>> {
        self.default_key.read().map(|g| g.clone()).map_err(|_| Self::poison_err("default_key"))
    }

    pub fn remove(&self, key: &str) -> BridgeResult<()> {
        self.pools.write().map_err(|_| Self::poison_err("pools"))?.remove(key);
        self.urls.write().map_err(|_| Self::poison_err("urls"))?.remove(key);
        let mut default = self.default_key.write().map_err(|_| Self::poison_err("default_key"))?;
        if default.as_deref() == Some(key) {
            *default = None;
        }
        Ok(())
    }

    pub fn contains(&self, key: &str) -> BridgeResult<bool> {
        self.pools.read().map(|g| g.contains_key(key)).map_err(|_| Self::poison_err("pools"))
    }
}

static POOL_MANAGER: once_cell::sync::Lazy<PoolManager> =
    once_cell::sync::Lazy::new(PoolManager::new);

pub fn pool_manager() -> &'static PoolManager {
    &POOL_MANAGER
}
