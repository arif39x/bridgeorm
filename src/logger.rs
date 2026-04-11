// every query in this file uses bound parameters.
use once_cell::sync::Lazy;
use std::sync::{Once, RwLock};
use tracing::{debug, warn, Level};
use tracing_subscriber::FmtSubscriber;

static INIT: Once = Once::new();
static SLOW_QUERY_THRESHOLD: Lazy<RwLock<u64>> = Lazy::new(|| RwLock::new(0));

#[derive(Debug)]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
}

pub struct QueryLog {
    pub sql: String,
    pub params: Vec<String>,
    pub duration_ms: u64,
    pub table: String,
}

pub fn configure_logging(level: &str, slow_query_ms: u64) {
    if let Ok(mut threshold) = SLOW_QUERY_THRESHOLD.write() {
        *threshold = slow_query_ms;
    }

    INIT.call_once(|| {
        let tracing_level = match level {
            "error" => Level::ERROR,
            "warn" => Level::WARN,
            "info" => Level::INFO,
            "debug" => Level::DEBUG,
            _ => Level::INFO,
        };

        if level != "off" {
            let subscriber = FmtSubscriber::builder()
                .with_max_level(tracing_level)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        }
    });
}

pub fn log_query(log: QueryLog) {
    let slow_query_limit = *SLOW_QUERY_THRESHOLD.read().unwrap_or(0);
    let message = format!(
        "[BridgeORM] {} | params=[{}] | duration={}ms | table={}",
        log.sql,
        log.params.join(", "),
        log.duration_ms,
        log.table
    );

    if log.duration_ms >= slow_query_limit && slow_query_limit > 0 {
        warn!("{}", message);
    } else {
        debug!("{}", message);
    }
}
