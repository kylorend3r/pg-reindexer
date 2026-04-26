/// Orchestrator module for high-level orchestration of reindexing operations

use crate::index_operations;
use crate::logging;
use crate::memory_table;
use crate::queries;
use crate::state;
use crate::types::{IndexFilterType, IndexInfo, LogStatement, SslMode};
use anyhow::Result;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio_postgres::Client;

fn analyze_sql_template_for_version(pg_version: i32) -> &'static str {
    if pg_version >= 140000 {
        queries::ANALYZE_TABLE_SKIP_LOCKED
    } else {
        queries::ANALYZE_TABLE
    }
}

fn resolve_analyze_targets(
    schemas: &[String],
    analyze_tables: &[String],
) -> Vec<(String, String)> {
    let mut targets = Vec::new();

    for analyze_target in analyze_tables {
        if let Some((schema, table)) = analyze_target.split_once('.') {
            let schema_name = schema.trim().to_string();
            let table_name = table.trim().to_string();
            if !schema_name.is_empty() && !table_name.is_empty() {
                targets.push((schema_name, table_name));
            }
            continue;
        }

        let table_name = analyze_target.trim().to_string();
        if table_name.is_empty() {
            continue;
        }

        for schema_name in schemas {
            targets.push((schema_name.clone(), table_name.clone()));
        }
    }

    targets
}

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
    pub sslmode: SslMode,
    pub ssl_ca_cert: Option<String>,
    pub ssl_client_cert: Option<String>,
    pub ssl_client_key: Option<String>,
    pub user_index_type: IndexFilterType,
    pub session_id: Option<String>,
    pub max_replica_lag_bytes: Option<i64>,
    pub max_replica_lag_wait_secs: Option<u64>,
    pub pacing_ms: u64,
    pub log_statement: LogStatement,
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
        cancel_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Vec<tokio::task::JoinHandle<Result<()>>> {
        let mut tasks = Vec::new();

        for worker_id in 0..effective_threads {
            let connection_string = self.connection_string.clone();
            let memory_table = memory_table.clone();
            let logger = self.logger.clone();
            let config = worker_config.clone();
            let cancel_rx = cancel_rx.clone();

            let task = tokio::spawn(async move {
                index_operations::worker_with_memory_table(
                    worker_id,
                    connection_string.to_string(),
                    memory_table,
                    logger,
                    config,
                    cancel_rx,
                )
                .await
            });

            tasks.push(task);
        }

        tasks
    }

    /// Wait for all worker tasks to complete and collect results
    /// Returns Ok((error_count, was_cancelled)) where was_cancelled indicates if timeout occurred
    pub async fn collect_worker_results(
        &self,
        tasks: Vec<tokio::task::JoinHandle<Result<()>>>,
        cancel_rx: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(usize, bool)> {
        // Default timeout: 30 seconds for graceful shutdown
        const GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 30;
        
        let logger = self.logger.clone();

        // Check if cancellation was already requested
        if *cancel_rx.borrow() {
            logger.log(
                logging::LogLevel::Info,
                "Cancellation requested, waiting for workers to complete gracefully...",
            );
        }

        // Wait for all tasks with timeout
        let result = tokio::time::timeout(
            tokio::time::Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECS),
            async {
                let mut error_count = 0;
                for (worker_id, task) in tasks.into_iter().enumerate() {
                    match task.await {
                        Ok(Ok(_)) => {
                            // Worker completed successfully
                            logger.log(
                                logging::LogLevel::Info,
                                &format!("Worker {} completed successfully", worker_id),
                            );
                        }
                        Ok(Err(e)) => {
                            error_count += 1;
                            eprintln!("  ✗ Worker {} failed: {}", worker_id, e);
                            logger.log(
                                logging::LogLevel::Error,
                                &format!("Worker {} failed: {}", worker_id, e),
                            );
                        }
                        Err(e) => {
                            error_count += 1;
                            eprintln!("  ✗ Worker {} panicked: {}", worker_id, e);
                            logger.log(
                                logging::LogLevel::Error,
                                &format!("Worker {} panicked: {}", worker_id, e),
                            );
                        }
                    }
                }
                error_count
            },
        )
        .await;

        match result {
            Ok(error_count) => {
                // All workers completed within timeout
                Ok((error_count, false))
            }
            Err(_) => {
                // Timeout occurred
                logger.log(
                    logging::LogLevel::Warning,
                    &format!(
                        "Timeout waiting for workers to complete ({} seconds). Some operations may have been interrupted.",
                        GRACEFUL_SHUTDOWN_TIMEOUT_SECS
                    ),
                );
                Ok((0, true))
            }
        }
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
        // Mark any indexes still Pending as Skipped (e.g. workers stopped due to replica lag timeout)
        memory_table.mark_all_pending_skipped().await;

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

    pub async fn run_post_analyze(
        &self,
        schemas: &[String],
        analyze_tables: &[String],
        completed_tables: &HashSet<(String, String)>,
    ) {
        let pg_version: i32 = match self.client.query_one(queries::GET_PG_VERSION_NUM, &[]).await {
            Ok(row) => row.get(0),
            Err(e) => {
                self.logger.log(
                    logging::LogLevel::Warning,
                    &format!("Skipping post-reindex ANALYZE: failed to get PostgreSQL version: {}", e),
                );
                return;
            }
        };

        let sql_template = analyze_sql_template_for_version(pg_version);
        let targets = resolve_analyze_targets(schemas, analyze_tables);

        for (schema_name, table_name) in targets {
            // Check 1: at least one index on this table was successfully reindexed
            if !completed_tables.contains(&(schema_name.clone(), table_name.clone())) {
                self.logger.log(
                    logging::LogLevel::Info,
                    &format!(
                        "Skipping ANALYZE for {}.{} — no indexes on this table were reindexed in this session",
                        schema_name, table_name
                    ),
                );
                continue;
            }

            // Check 2: table exists in the database
            match crate::schema::table_exists(&self.client, &schema_name, &table_name).await {
                Ok(true) => {}
                Ok(false) => {
                    self.logger.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Skipping ANALYZE for {}.{} — table does not exist",
                            schema_name, table_name
                        ),
                    );
                    continue;
                }
                Err(e) => {
                    self.logger.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Skipping ANALYZE for {}.{} — failed to check table existence: {}",
                            schema_name, table_name, e
                        ),
                    );
                    continue;
                }
            }

            self.logger.log(
                logging::LogLevel::Info,
                &format!("Running post-reindex ANALYZE on {}.{}", schema_name, table_name),
            );

            let query = sql_template
                .replacen("{}", &schema_name, 1)
                .replacen("{}", &table_name, 1);
            match self.client.execute(&query, &[]).await {
                Ok(_) => self.logger.log(
                    logging::LogLevel::Success,
                    &format!("ANALYZE completed for {}.{}", schema_name, table_name),
                ),
                Err(e) => self.logger.log(
                    logging::LogLevel::Warning,
                    &format!("ANALYZE failed for {}.{}: {}", schema_name, table_name, e),
                ),
            }
        }
    }
}


