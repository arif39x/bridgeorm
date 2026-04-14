use thiserror::Error;

/// Unified error enum for the entire BridgeORM library.
/// Rule: One unified error enum in error.rs using thiserror.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BridgeOrmError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Type alias for Results returned by BridgeORM functions.
pub type BridgeOrmResult<T> = Result<T, BridgeOrmError>;
