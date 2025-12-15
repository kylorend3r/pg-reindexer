/// Orchestrator module for high-level orchestration of reindexing operations

use crate::index_operations;
use crate::logging;
use crate::memory_table;
use crate::state;
use crate::types::{IndexFilterType, IndexInfo};
use anyhow::Result;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio_postgres::Client;

/// Worker configuration for creating worker tasks
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub maintenance_work_mem_gb: u64,
    pub max_parallel_maintenance_workers: u64,
    pub maintenance_io_concurrency: u64,
    pub lock_timeout_seconds: u64,
    pub skip_inactive_replication_slots: bool,
    pub skip_sync_replication_connection: bool,
    pub skip_active_vacuums: bool,
    pub bloat_threshold: Option<u8>,
    pub concurrently: bool,
    pub use_ssl: bool,
    pub accept_invalid_certs: bool,
    pub ssl_ca_cert: Option<String>,
    pub ssl_client_cert: Option<String>,
    pub ssl_client_key: Option<String>,
    pub user_index_type: IndexFilterType,
    pub session_id: Option<String>,
}

/// Orchestrator for managing reindexing operations
pub struct ReindexOrchestrator {
    client: Client,
    logger: Arc<logging::Logger>,
    connection_string: Arc<String>,
}

impl ReindexOrchestrator {
    /// Create a new ReindexOrchestrator
    pub fn new(
        client: Client,
        logger: Arc<logging::Logger>,
        connection_string: Arc<String>,
        _connection_config: crate::connection::ConnectionConfig,
    ) -> Self {
        Self {
            client,
            logger,
            connection_string,
        }
    }

    /// Clean orphaned _ccnew indexes if requested
    pub async fn clean_orphaned_indexes(
        &self,
        indexes: &[IndexInfo],
        clean_orphaned: bool,
    ) -> Result<()> {
        if !clean_orphaned {
            return Ok(());
        }

        self.logger.log(
            logging::LogLevel::Info,
            "Cleaning orphaned _ccnew indexes...",
        );

        for index in indexes {
            if index_operations::is_temporary_concurrent_reindex_index(&index.index_name)
                && let Err(e) = index_operations::clean_orphaned_ccnew_index(
                    &self.client,
                    &index.schema_name,
                    &index.index_name,
                    &self.logger,
                )
                .await
            {
                self.logger.log(
                    logging::LogLevel::Error,
                    &format!(
                        "Failed to drop orphaned index {}.{}: {}",
                        index.schema_name, index.index_name, e
                    ),
                );
            }
        }

        Ok(())
    }

    /// Process excluded indexes and filter indexes
    pub async fn process_and_filter_indexes(
        &self,
        indexes: Vec<IndexInfo>,
        excluded_indexes: &HashSet<String>,
        index_type: IndexFilterType,
    ) -> Result<Vec<IndexInfo>> {
        // Save excluded indexes to logbook before filtering them out
        index_operations::save_excluded_indexes_to_logbook(
            &self.client,
            &indexes,
            excluded_indexes,
            index_type,
            &self.logger,
        )
        .await?;

        // Filter out excluded indexes and orphaned _ccnew indexes from processing
        let filtered_indexes =
            index_operations::filter_indexes(indexes, excluded_indexes, &self.logger);

        Ok(filtered_indexes)
    }

    /// Initialize state table if needed
    pub async fn initialize_state_if_needed(
        &self,
        resume: bool,
        filtered_indexes: &[IndexInfo],
        session_id: Option<&str>,
    ) -> Result<()> {
        // Initialize state table if not resuming (if resuming, it was already initialized)
        if !resume {
            if let Some(sid) = session_id {
                if let Err(e) = state::initialize_state_table(&self.client, filtered_indexes, sid).await {
                    self.logger.log(
                        logging::LogLevel::Warning,
                        &format!("Failed to initialize state table: {}", e),
                    );
                }
            }
        }

        Ok(())
    }

    /// Create and spawn worker tasks
    pub fn create_worker_tasks(
        &self,
        effective_threads: usize,
        memory_table: Arc<memory_table::SharedIndexMemoryTable>,
        worker_config: WorkerConfig,
    ) -> Vec<tokio::task::JoinHandle<Result<()>>> {
        let mut tasks = Vec::new();

        for worker_id in 0..effective_threads {
            let connection_string = self.connection_string.clone();
            let memory_table = memory_table.clone();
            let logger = self.logger.clone();
            let config = worker_config.clone();

            let task = tokio::spawn(async move {
                index_operations::worker_with_memory_table(
                    worker_id,
                    connection_string.to_string(),
                    memory_table,
                    logger,
                    config.maintenance_work_mem_gb,
                    config.max_parallel_maintenance_workers,
                    config.maintenance_io_concurrency,
                    config.lock_timeout_seconds,
                    config.skip_inactive_replication_slots,
                    config.skip_sync_replication_connection,
                    config.skip_active_vacuums,
                    config.bloat_threshold,
                    config.concurrently,
                    config.use_ssl,
                    config.accept_invalid_certs,
                    config.ssl_ca_cert,
                    config.ssl_client_cert,
                    config.ssl_client_key,
                    config.user_index_type,
                    config.session_id,
                )
                .await
            });

            tasks.push(task);
        }

        tasks
    }

    /// Wait for all worker tasks to complete and collect results
    pub async fn collect_worker_results(
        &self,
        tasks: Vec<tokio::task::JoinHandle<Result<()>>>,
    ) -> Result<usize> {
        let mut error_count = 0;

        for (worker_id, task) in tasks.into_iter().enumerate() {
            match task.await {
                Ok(Ok(_)) => {
                    // Worker completed successfully
                    self.logger.log(
                        logging::LogLevel::Info,
                        &format!("Worker {} completed successfully", worker_id),
                    );
                }
                Ok(Err(e)) => {
                    error_count += 1;
                    eprintln!("  ✗ Worker {} failed: {}", worker_id, e);
                    self.logger.log(
                        logging::LogLevel::Error,
                        &format!("Worker {} failed: {}", worker_id, e),
                    );
                }
                Err(e) => {
                    error_count += 1;
                    eprintln!("  ✗ Worker {} panicked: {}", worker_id, e);
                    self.logger.log(
                        logging::LogLevel::Error,
                        &format!("Worker {} panicked: {}", worker_id, e),
                    );
                }
            }
        }

        Ok(error_count)
    }

    /// Get final statistics and log completion message
    pub async fn finalize_and_log_completion(
        &self,
        memory_table: Arc<memory_table::SharedIndexMemoryTable>,
        start_time: Instant,
        effective_threads: usize,
        log_file: String,
        silence_mode: bool,
    ) {
        // Get final statistics from memory table
        let (pending, in_progress, completed, failed, skipped) =
            memory_table.get_statistics().await;

        let duration = start_time.elapsed();

        // Create a new logger for the final message (with silence mode)
        let final_logger = logging::Logger::new_with_silence(log_file, silence_mode);
        let total_processed = completed + failed + skipped + pending + in_progress;
        final_logger.log_completion_message(total_processed, failed, duration, effective_threads);

        if !silence_mode {
            final_logger.log(logging::LogLevel::Success, "Reindex process completed");
        }
    }
}

