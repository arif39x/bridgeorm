use crate::engine::query::QueryValue;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct EntitySnapshot {
    pub table_name: String,
    pub values: HashMap<String, QueryValue>,
}

pub struct DirtyTracker {
    pub snapshots: HashMap<String, EntitySnapshot>,
}

impl DirtyTracker {
    pub fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
        }
    }

    pub fn take_snapshot(
        &mut self,
        key: String,
        table_name: String,
        values: HashMap<String, QueryValue>,
    ) {
        self.snapshots
            .insert(key, EntitySnapshot { table_name, values });
    }

    pub fn remove_snapshot(&mut self, key: &str) {
        self.snapshots.remove(key);
    }

    pub fn compute_diff(
        &self,
        key: &str,
        current_values: &HashMap<String, QueryValue>,
    ) -> Option<HashMap<String, QueryValue>> {
        let snapshot = self.snapshots.get(key)?;
        let mut diff = HashMap::new();

        for (col, current_val) in current_values {
            if let Some(original_val) = snapshot.values.get(col) {
                if current_val != original_val {
                    diff.insert(col.clone(), current_val.clone());
                }
            }
        }

        if diff.is_empty() {
            None
        } else {
            Some(diff)
        }
    }
}
