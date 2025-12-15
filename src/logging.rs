use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
    Success,
}

// Buffer size for BufWriter (8KB default)
const LOG_BUFFER_SIZE: usize = 8192;

pub struct Logger {
    log_file: String,
    silence_mode: bool,
    file_handle: Arc<Mutex<Option<BufWriter<File>>>>,
}

impl Logger {
    #[allow(dead_code)]
    pub fn new(log_file: String) -> Self {
        Self::new_with_silence(log_file, false)
    }

    pub fn new_with_silence(log_file: String, silence_mode: bool) -> Self {
        Self {
            log_file,
            silence_mode,
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
        // Format message before acquiring lock to minimize lock hold time
        let timestamp = self.get_timestamp();
        let level_str = match level {
            LogLevel::Info => "INFO",
            LogLevel::Warning => "WARN",
            LogLevel::Error => "ERROR",
            LogLevel::Success => "SUCCESS",
        };

        let formatted_message = format!("[{}] [{}] {}\n", timestamp, level_str, message);

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
        // Format message before acquiring lock to minimize lock hold time
        let timestamp = self.get_timestamp();
        let level_str = match level {
            LogLevel::Info => "INFO",
            LogLevel::Warning => "WARN",
            LogLevel::Error => "ERROR",
            LogLevel::Success => "SUCCESS",
        };

        let formatted_message = format!("[{}] [{}] {}\n", timestamp, level_str, message);

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
        self.log(
            LogLevel::Info,
            &format!(
                "[{}/{}] Starting reindex: {}.{} ({})",
                index_num + 1,
                total,
                schema,
                index,
                index_type
            ),
        );
    }

    pub fn log_index_success(&self, schema: &str, index: &str) {
        self.log(
            LogLevel::Success,
            &format!("Reindexed {}.{} successfully", schema, index),
        );
    }

    pub fn log_index_validation_failed(&self, schema: &str, index: &str) {
        self.log(
            LogLevel::Error,
            &format!(
                "{}.{} - Index integrity check failed after reindexing",
                schema, index
            ),
        );
    }

    pub fn log_index_failed(&self, schema: &str, index: &str, reason: &str) {
        self.log(
            LogLevel::Error,
            &format!("{}.{} - Reindex failed: {}", schema, index, reason),
        );
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
        self.log_always(LogLevel::Success, &summary);
        
        // Also log detailed stats to file
        if self.silence_mode {
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
        self.log(
            LogLevel::Info,
            &format!("Maintenance work mem: {} GB", maintenance_work_mem_gb),
        );
        self.log(
            LogLevel::Info,
            &format!(
                "Max parallel maintenance workers: {}",
                max_parallel_maintenance_workers
            ),
        );
        self.log(
            LogLevel::Info,
            &format!("Maintenance IO concurrency: {}", maintenance_io_concurrency),
        );
        self.log(
            LogLevel::Info,
            &format!("Lock timeout: {} seconds", lock_timeout_seconds),
        );
    }

    pub fn log_index_size_limits(&self, min_size_gb: u64, max_size_gb: u64) {
        self.log(
            LogLevel::Info,
            &format!(
                "Index size limits: minimum {} GB, maximum {} GB",
                min_size_gb, max_size_gb
            ),
        );
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

