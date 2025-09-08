use std::{fs, io::Write};

#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
    Success,
}

pub struct Logger {
    log_file: String,
}

impl Logger {
    pub fn new(log_file: String) -> Self {
        Self { log_file }
    }

    pub fn log(&self, level: LogLevel, message: &str) {
        let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S");
        let level_str = match level {
            LogLevel::Info => "INFO",
            LogLevel::Warning => "WARN",
            LogLevel::Error => "ERROR",
            LogLevel::Success => "SUCCESS",
        };

        let formatted_message = format!("[{}] [{}] {}", timestamp, level_str, message);

        // Print to stdout
        println!("{}", formatted_message);

        // Write to log file
        if let Ok(mut file) = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)
        {
            let _ = writeln!(file, "{}", formatted_message);
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

    pub fn log_index_skipped(&self, schema: &str, index: &str, reason: &str) {
        self.log(
            LogLevel::Warning,
            &format!("{}.{} - {}", schema, index, reason),
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
            &format!(
                "{}.{} - Reindex failed: {}",
                schema, index, reason
            ),
        );
    }

    pub fn log_completion_message(
        &self,
        total: usize,
        failed: usize,
        duration: std::time::Duration,
        threads: usize,
    ) {
        self.log(LogLevel::Info, "=== REINDEX COMPLETED ===");
        self.log(
            LogLevel::Info,
            &format!("Total indexes processed: {}", total),
        );
        if failed > 0 {
            self.log(LogLevel::Error, &format!("Failed: {}", failed));
        }
        self.log(LogLevel::Info, &format!("Duration: {:.2?}", duration));
        self.log(
            LogLevel::Info,
            &format!("Concurrent threads used: {}", threads),
        );
        self.log(
            LogLevel::Info,
            "For detailed results, review the reindex_logbook table:",
        );
        self.log(
            LogLevel::Info,
            "SELECT * FROM reindexer.reindex_logbook WHERE reindex_time >= NOW() - INTERVAL '1 hour' ORDER BY reindex_time DESC;",
        );
    }

    pub fn log_dry_run(&self, indexes: &[crate::IndexInfo]) {
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
    }

    pub fn log_index_size_limits(&self, min_size_gb: u64, max_size_gb: u64) {
        self.log(
            LogLevel::Info,
            &format!("Index size limits: minimum {} GB, maximum {} GB", min_size_gb, max_size_gb),
        );
    }
}