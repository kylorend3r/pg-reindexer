use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};
use serde::Serialize;
use crate::types::LogFormat;

#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
    Success,
}

impl LogLevel {
    fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Info => "INFO",
            LogLevel::Warning => "WARN",
            LogLevel::Error => "ERROR",
            LogLevel::Success => "SUCCESS",
        }
    }
}

/// Context for structured logging with optional fields
#[derive(Default)]
pub struct LogContext<'a> {
    pub schema: Option<&'a str>,
    pub index_name: Option<&'a str>,
    pub index_type: Option<&'a str>,
    pub status: Option<&'a str>,
    pub duration_secs: Option<f64>,
    pub total_indexes: Option<usize>,
    pub current_index: Option<usize>,
    pub failed_count: Option<usize>,
    pub thread_count: Option<usize>,
    pub error: Option<&'a str>,
    pub maintenance_work_mem_gb: Option<u64>,
    pub max_parallel_maintenance_workers: Option<u64>,
    pub maintenance_io_concurrency: Option<u64>,
    pub lock_timeout_seconds: Option<u64>,
    pub min_size_gb: Option<u64>,
    pub max_size_gb: Option<u64>,
}

/// JSON log event structure
#[derive(Serialize)]
struct LogEvent<'a> {
    timestamp: &'a str,
    level: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_secs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_indexes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maintenance_work_mem_gb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_parallel_maintenance_workers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maintenance_io_concurrency: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lock_timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_size_gb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_size_gb: Option<u64>,
}

// Buffer size for BufWriter (8KB default)
const LOG_BUFFER_SIZE: usize = 8192;

pub struct Logger {
    log_file: String,
    silence_mode: bool,
    json_mode: bool,
    file_handle: Arc<Mutex<Option<BufWriter<File>>>>,
}

impl Logger {
    pub fn new(log_file: String, silence_mode: bool, log_format: LogFormat) -> Self {
        Self {
            log_file,
            silence_mode,
            json_mode: log_format == LogFormat::Json,
            file_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn new_with_silence(log_file: String, silence_mode: bool) -> Self {
        Self {
            log_file,
            silence_mode,
            json_mode: false,
            file_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Ensure the file handle is initialized, creating it if necessary.
    /// Returns true if the file handle is available, false otherwise.
    fn ensure_file_handle(&self) -> bool {
        let mut guard = match self.file_handle.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Handle lock poisoning - use the poisoned lock
                eprintln!("Logger lock was poisoned, attempting recovery");
                poisoned.into_inner()
            }
        };

        // If file handle already exists, we're good
        if guard.is_some() {
            return true;
        }

        // Try to open/create the log file
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)
        {
            Ok(file) => {
                let writer = BufWriter::with_capacity(LOG_BUFFER_SIZE, file);
                *guard = Some(writer);
                true
            }
            Err(e) => {
                eprintln!(
                    "Failed to open log file '{}': {}. Logging to file will be disabled.",
                    self.log_file, e
                );
                false
            }
        }
    }

    fn get_timestamp(&self) -> String {
        chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string()
    }

    pub fn log(&self, level: LogLevel, message: &str) {
        self.log_with_context(level, message, LogContext::default());
    }

    pub fn log_with_context(&self, level: LogLevel, message: &str, context: LogContext<'_>) {
        // Format message before acquiring lock to minimize lock hold time
        let timestamp = self.get_timestamp();
        let level_str = level.as_str();

        let formatted_message = if self.json_mode {
            let event = LogEvent {
                timestamp: &timestamp,
                level: level_str,
                message,
                schema: context.schema,
                index_name: context.index_name,
                index_type: context.index_type,
                status: context.status,
                duration_secs: context.duration_secs,
                total_indexes: context.total_indexes,
                current_index: context.current_index,
                failed_count: context.failed_count,
                thread_count: context.thread_count,
                error: context.error,
                maintenance_work_mem_gb: context.maintenance_work_mem_gb,
                max_parallel_maintenance_workers: context.max_parallel_maintenance_workers,
                maintenance_io_concurrency: context.maintenance_io_concurrency,
                lock_timeout_seconds: context.lock_timeout_seconds,
                min_size_gb: context.min_size_gb,
                max_size_gb: context.max_size_gb,
            };
            format!("{}\n", serde_json::to_string(&event).unwrap_or_else(|_| {
                // Fallback to text format on serialization error
                format!("{{\"timestamp\":\"{}\",\"level\":\"{}\",\"message\":\"{}\"}}", timestamp, level_str, message)
            }))
        } else {
            format!("[{}] [{}] {}\n", timestamp, level_str, message)
        };

        // Print to stdout only if not in silence mode
        if !self.silence_mode {
            print!("{}", formatted_message);
        }

        // Write to log file using cached handle
        if self.ensure_file_handle() {
            let mut guard = match self.file_handle.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    eprintln!("Logger lock was poisoned during write");
                    poisoned.into_inner()
                }
            };

            if let Some(ref mut writer) = *guard {
                if let Err(e) = writer.write_all(formatted_message.as_bytes()) {
                    eprintln!("Failed to write to log file '{}': {}", self.log_file, e);
                    // Try to recover by resetting the file handle
                    *guard = None;
                } else {
                    // Auto-flush on Error level to ensure critical messages are written immediately
                    if matches!(level, LogLevel::Error) {
                        if let Err(e) = writer.flush() {
                            eprintln!("Failed to flush log file '{}': {}", self.log_file, e);
                        }
                    }
                }
            }
        }
    }

    /// Log to both terminal and file (always prints, even in silence mode)
    pub fn log_always(&self, level: LogLevel, message: &str) {
        self.log_always_with_context(level, message, LogContext::default());
    }

    /// Log to both terminal and file with context (always prints, even in silence mode)
    pub fn log_always_with_context(&self, level: LogLevel, message: &str, context: LogContext<'_>) {
        // Format message before acquiring lock to minimize lock hold time
        let timestamp = self.get_timestamp();
        let level_str = level.as_str();

        let formatted_message = if self.json_mode {
            let event = LogEvent {
                timestamp: &timestamp,
                level: level_str,
                message,
                schema: context.schema,
                index_name: context.index_name,
                index_type: context.index_type,
                status: context.status,
                duration_secs: context.duration_secs,
                total_indexes: context.total_indexes,
                current_index: context.current_index,
                failed_count: context.failed_count,
                thread_count: context.thread_count,
                error: context.error,
                maintenance_work_mem_gb: context.maintenance_work_mem_gb,
                max_parallel_maintenance_workers: context.max_parallel_maintenance_workers,
                maintenance_io_concurrency: context.maintenance_io_concurrency,
                lock_timeout_seconds: context.lock_timeout_seconds,
                min_size_gb: context.min_size_gb,
                max_size_gb: context.max_size_gb,
            };
            format!("{}\n", serde_json::to_string(&event).unwrap_or_else(|_| {
                format!("{{\"timestamp\":\"{}\",\"level\":\"{}\",\"message\":\"{}\"}}", timestamp, level_str, message)
            }))
        } else {
            format!("[{}] [{}] {}\n", timestamp, level_str, message)
        };

        // Always print to stdout
        print!("{}", formatted_message);

        // Write to log file using cached handle
        if self.ensure_file_handle() {
            let mut guard = match self.file_handle.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    eprintln!("Logger lock was poisoned during write");
                    poisoned.into_inner()
                }
            };

            if let Some(ref mut writer) = *guard {
                if let Err(e) = writer.write_all(formatted_message.as_bytes()) {
                    eprintln!("Failed to write to log file '{}': {}", self.log_file, e);
                    // Try to recover by resetting the file handle
                    *guard = None;
                } else {
                    // Auto-flush on Error level to ensure critical messages are written immediately
                    if matches!(level, LogLevel::Error) {
                        if let Err(e) = writer.flush() {
                            eprintln!("Failed to flush log file '{}': {}", self.log_file, e);
                        }
                    }
                }
            }
        }
    }

    pub fn log_index_start(
        &self,
        index_num: usize,
        total: usize,
        schema: &str,
        index: &str,
        index_type: &str,
    ) {
        let message = format!(
            "[{}/{}] Starting reindex: {}.{} ({})",
            index_num + 1,
            total,
            schema,
            index,
            index_type
        );
        let context = LogContext {
            schema: Some(schema),
            index_name: Some(index),
            index_type: Some(index_type),
            status: Some("starting"),
            current_index: Some(index_num + 1),
            total_indexes: Some(total),
            ..Default::default()
        };
        self.log_with_context(LogLevel::Info, &message, context);
    }

    pub fn log_index_success(&self, schema: &str, index: &str) {
        let message = format!("Reindexed {}.{} successfully", schema, index);
        let context = LogContext {
            schema: Some(schema),
            index_name: Some(index),
            status: Some("success"),
            ..Default::default()
        };
        self.log_with_context(LogLevel::Success, &message, context);
    }

    pub fn log_index_validation_failed(&self, schema: &str, index: &str) {
        let message = format!(
            "{}.{} - Index integrity check failed after reindexing",
            schema, index
        );
        let context = LogContext {
            schema: Some(schema),
            index_name: Some(index),
            status: Some("validation_failed"),
            error: Some("Index integrity check failed after reindexing"),
            ..Default::default()
        };
        self.log_with_context(LogLevel::Error, &message, context);
    }

    pub fn log_index_failed(&self, schema: &str, index: &str, reason: &str) {
        let message = format!("{}.{} - Reindex failed: {}", schema, index, reason);
        let context = LogContext {
            schema: Some(schema),
            index_name: Some(index),
            status: Some("failed"),
            error: Some(reason),
            ..Default::default()
        };
        self.log_with_context(LogLevel::Error, &message, context);
    }

    pub fn log_completion_message(
        &self,
        total: usize,
        failed: usize,
        duration: std::time::Duration,
        threads: usize,
    ) {
        // Always print completion message (even in silence mode)
        let summary = format!(
            "Reindexing completed: {} total indexes processed, {} failed, Duration: {:.2?} (using {} threads)",
            total, failed, duration, threads
        );
        let context = LogContext {
            status: Some("completed"),
            total_indexes: Some(total),
            failed_count: Some(failed),
            duration_secs: Some(duration.as_secs_f64()),
            thread_count: Some(threads),
            ..Default::default()
        };
        self.log_always_with_context(LogLevel::Success, &summary, context);

        // Also log detailed stats to file (only in text mode for silence mode)
        if self.silence_mode && !self.json_mode {
            self.log(
                LogLevel::Info,
                &format!("Total indexes processed: {}", total),
            );
            self.log(LogLevel::Info, &format!("Failed indexes: {}", failed));
            self.log(LogLevel::Info, &format!("Duration: {:.2?}", duration));
            self.log(LogLevel::Info, &format!("Threads used: {}", threads));
        }
    }

    pub fn log_dry_run(&self, indexes: &[crate::types::IndexInfo]) {
        self.log(LogLevel::Info, "=== DRY RUN MODE ===");
        self.log(
            LogLevel::Info,
            "No indexes will be reindexed. The following commands would be executed:",
        );

        for (i, index) in indexes.iter().enumerate() {
            let reindex_sql = format!(
                "REINDEX INDEX CONCURRENTLY \"{}\".\"{}\"",
                index.schema_name, index.index_name
            );
            self.log(
                LogLevel::Info,
                &format!("[{}/{}] {}", i + 1, indexes.len(), reindex_sql),
            );
        }

        self.log(
            LogLevel::Info,
            &format!("Total indexes to reindex: {}", indexes.len()),
        );
        self.log(
            LogLevel::Info,
            "HINT: To actually reindex, run without --dry-run flag",
        );
    }

    pub fn log_session_parameters(
        &self,
        maintenance_work_mem_gb: u64,
        max_parallel_maintenance_workers: u64,
        maintenance_io_concurrency: u64,
        lock_timeout_seconds: u64,
    ) {
        let message = format!(
            "Session parameters: maintenance_work_mem={}GB, max_parallel_maintenance_workers={}, maintenance_io_concurrency={}, lock_timeout={}s",
            maintenance_work_mem_gb, max_parallel_maintenance_workers, maintenance_io_concurrency, lock_timeout_seconds
        );
        let context = LogContext {
            maintenance_work_mem_gb: Some(maintenance_work_mem_gb),
            max_parallel_maintenance_workers: Some(max_parallel_maintenance_workers),
            maintenance_io_concurrency: Some(maintenance_io_concurrency),
            lock_timeout_seconds: Some(lock_timeout_seconds),
            ..Default::default()
        };
        self.log_with_context(LogLevel::Info, &message, context);
    }

    pub fn log_index_size_limits(&self, min_size_gb: u64, max_size_gb: u64) {
        let message = format!(
            "Index size limits: minimum {} GB, maximum {} GB",
            min_size_gb, max_size_gb
        );
        let context = LogContext {
            min_size_gb: Some(min_size_gb),
            max_size_gb: Some(max_size_gb),
            ..Default::default()
        };
        self.log_with_context(LogLevel::Info, &message, context);
    }

}

impl Drop for Logger {
    fn drop(&mut self) {
        // Flush and close the file handle on drop
        let mut guard = match self.file_handle.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Even if poisoned, try to flush
                poisoned.into_inner()
            }
        };

        if let Some(mut writer) = guard.take() {
            // Flush the buffer before dropping
            if let Err(e) = writer.flush() {
                eprintln!("Failed to flush log file '{}' during drop: {}", self.log_file, e);
            }
            // File closes automatically when writer is dropped
        }
    }
}

