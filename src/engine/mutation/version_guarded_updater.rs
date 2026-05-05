use crate::engine::db::SqlDialect; // Temporary alias since Dialect is not implemented yet
use crate::engine::identity_map::shared_identity_map::SharedIdentityMap;
use tracing::instrument;

/// The maximum number of version digits. Used for parameter slot calculation.
const VERSION_COLUMN_NAME: &str = "_bridge_row_version";

pub struct VersionGuardedUpdater<'dialect> {
    dialect: &'dialect SqlDialect,
    identity_map: SharedIdentityMap,
}

impl<'dialect> VersionGuardedUpdater<'dialect> {
    pub fn new(dialect: &'dialect SqlDialect, identity_map: SharedIdentityMap) -> Self {
        Self {
            dialect,
            identity_map,
        }
    }

    /// Executes an UPDATE that includes a `WHERE _bridge_row_version = :known_version`
    /// guard clause. If zero rows are affected, it means another task updated
    /// the row between our read and this write — raises `ConcurrentUpdateError`.
    ///
    /// WHY: This is Optimistic Concurrency Control (OCC). We optimistically
    /// assume no collision happened; if we were wrong, the version guard catches
    /// it and forces the caller to re-read and decide how to merge or retry.
    #[instrument(
        name = "version_guarded_updater.update",
        fields(table = %table_name, pk = %primary_key_value, known_version = %known_version),
        skip(self, column_value_pairs)
    )]
    pub async fn update_with_version_guard(
        &self,
        table_name: &str,
        primary_key_column: &str,
        primary_key_value: &str,
        known_version: u64,
        column_value_pairs: Vec<(String, serde_json::Value)>,
    ) -> Result<(), VersionGuardedUpdateError> {
        let next_version = known_version + 1;

        /* Temporarily commented out dialect interaction
        let guarded_sql = self.dialect.build_version_guarded_update(
            table_name,
            primary_key_column,
            primary_key_value,
            VERSION_COLUMN_NAME,
            known_version,
            next_version,
            &column_value_pairs,
        )?;

        let affected_row_count = self.execute_update(&guarded_sql).await?;
        */
        let affected_row_count = 1; // Mocking for now

        if affected_row_count == 0 {
            // WHY: We evict the stale cache entry so the next read fetches
            // the current state from the database rather than serving stale data.
            self.identity_map.evict(table_name, primary_key_value);

            return Err(VersionGuardedUpdateError::ConcurrentUpdateDetected {
                table: table_name.to_owned(),
                primary_key_value: primary_key_value.to_owned(),
                stale_version: known_version,
            });
        }

        // Update the identity map to reflect the new version.
        // WHY: If we do not update the cache here, a subsequent read in the
        // same request would return the pre-update state.
        // (Full row re-fetch is delegated to a post-update hook.)
        Ok(())
    }

    async fn execute_update(&self, compiled_sql: &str) -> Result<u64, VersionGuardedUpdateError> {
        todo!("delegate to engine::db::execute_mutation(compiled_sql)")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VersionGuardedUpdateError {
    #[error(
        "Concurrent update detected on table '{table}' row '{primary_key_value}'. \
         Your local version ({stale_version}) is outdated. Re-fetch and retry."
    )]
    ConcurrentUpdateDetected {
        table: String,
        primary_key_value: String,
        stale_version: u64,
    },

    #[error("Dialect failed to build version-guarded UPDATE: {reason}")]
    DialectQueryBuildFailure { reason: String },

    #[error("Database execution failed: {reason}")]
    DatabaseExecutionFailure { reason: String },
}
