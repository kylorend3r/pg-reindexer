use crate::config::{DEFAULT_RETRY_DELAY_MS, MAX_REINDEX_RETRIES, REINDEX_RETRY_DELAY_SECS};
use crate::logging;
use crate::memory_table::SharedIndexMemoryTable;
use crate::orchestrator::WorkerConfig;
use crate::types::{IndexInfo, IndexFilterType, PartitionedTableInfo, PartitionInfo};
use anyhow::{Context, Result};
use std::sync::Arc;

/// Check if an index name matches PostgreSQL's temporary concurrent reindex pattern
/// This matches names like "_ccnew", "_ccnew1", "_ccnew2", etc.
pub fn is_temporary_concurrent_reindex_index(index_name: &str) -> bool {
    if let Some(ccnew_pos) = index_name.find("_ccnew") {
        // Check if the part after "_ccnew" is either empty or consists only of digits
        let after_ccnew = &index_name[ccnew_pos + 6..];
        after_ccnew.is_empty() || after_ccnew.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

pub async fn get_indexes_in_schema(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: Option<&str>,
    min_size_gb: u64,
    max_size_gb: u64,
    index_type: IndexFilterType,
    order_by_size: Option<&str>,
) -> Result<Vec<IndexInfo>> {
    let has_table_filter = table_name.is_some();
    let query = crate::queries::build_indexes_query(has_table_filter, index_type, order_by_size);

    let rows = if let Some(table) = table_name {
        client
            .query(
                &query,
                &[
                    &schema_name,
                    &table,
                    &(min_size_gb as i64),
                    &(max_size_gb as i64),
                ],
            )
            .await
    } else {
        client
            .query(
                &query,
                &[&schema_name, &(min_size_gb as i64), &(max_size_gb as i64)],
            )
            .await
    }
    .context("Failed to query indexes in schema")?;

    let mut indexes = Vec::new();

    for row in rows {
        let index = IndexInfo {
            schema_name: row.get(0),
            index_name: row.get(2),
            index_type: row.get(4),
            table_name: row.get(1),
            size_bytes: row.get(5), // size_bytes is now column 5
            parent_table_name: None, // Regular indexes don't have a parent table
        };
        indexes.push(index);
    }

    Ok(indexes)
}

/// Get indexes from multiple schemas
pub async fn get_indexes_in_schemas(
    client: &tokio_postgres::Client,
    schema_names: &[String],
    table_name: Option<&str>,
    min_size_gb: u64,
    max_size_gb: u64,
    index_type: IndexFilterType,
    order_by_size: Option<&str>,
) -> Result<Vec<IndexInfo>> {
    let mut all_indexes = Vec::new();

    for schema_name in schema_names {
        let indexes = get_indexes_in_schema(
            client,
            schema_name,
            table_name,
            min_size_gb,
            max_size_gb,
            index_type,
            order_by_size,
        )
        .await?;
        all_indexes.extend(indexes);
    }

    Ok(all_indexes)
}

/// Check if a table is partitioned (has child partitions)
pub async fn is_table_partitioned(
    client: &tokio_postgres::Client,
    table_name: &str,
    schema_name: &str,
) -> Result<bool> {
    let rows = client
        .query(
            crate::queries::CHECK_TABLE_IS_PARTITIONED,
            &[&table_name, &schema_name],
        )
        .await
        .context("Failed to check if table is partitioned")?;

    if let Some(row) = rows.first() {
        let is_partitioned: bool = row.get(0);
        Ok(is_partitioned)
    } else {
        Ok(false)
    }
}

/// Get all partitions of a partitioned table
pub async fn get_table_partitions(
    client: &tokio_postgres::Client,
    table_name: &str,
    schema_name: &str,
) -> Result<Vec<PartitionInfo>> {
    let rows = client
        .query(
            crate::queries::GET_TABLE_PARTITIONS,
            &[&table_name, &schema_name],
        )
        .await
        .context("Failed to get table partitions")?;

    let mut partitions = Vec::new();
    for row in rows {
        partitions.push(PartitionInfo {
            schema_name: row.get(0),
            partition_name: row.get(1),
        });
    }

    Ok(partitions)
}

/// Get all partitioned tables in a schema
pub async fn get_partitioned_tables_in_schema(
    client: &tokio_postgres::Client,
    schema_name: &str,
) -> Result<Vec<PartitionedTableInfo>> {
    let rows = client
        .query(
            crate::queries::GET_PARTITIONED_TABLES_IN_SCHEMA,
            &[&schema_name],
        )
        .await
        .context("Failed to get partitioned tables in schema")?;

    let mut tables = Vec::new();
    for row in rows {
        tables.push(PartitionedTableInfo {
            schema_name: row.get(0),
            table_name: row.get(1),
            partition_count: row.get(2),
        });
    }

    Ok(tables)
}

/// Get indexes for all partitions of a partitioned table
pub async fn get_partitioned_table_indexes(
    client: &tokio_postgres::Client,
    table_name: &str,
    schema_name: &str,
    min_size_gb: u64,
    max_size_gb: u64,
) -> Result<Vec<IndexInfo>> {
    let rows = client
        .query(
            crate::queries::GET_PARTITIONED_TABLE_INDEXES,
            &[&table_name, &schema_name],
        )
        .await
        .context("Failed to get partitioned table indexes")?;

    let min_size_bytes = min_size_gb as i64 * 1024 * 1024 * 1024;
    let max_size_bytes = max_size_gb as i64 * 1024 * 1024 * 1024;

    let mut indexes = Vec::new();
    for row in rows {
        let size_bytes: i64 = row.get(5);

        // Apply size filter
        if size_bytes >= min_size_bytes && size_bytes < max_size_bytes {
            indexes.push(IndexInfo {
                schema_name: row.get(0),
                table_name: row.get(1),
                index_name: row.get(2),
                index_type: row.get(4),
                size_bytes: Some(size_bytes),
                parent_table_name: Some(row.get(6)),
            });
        }
    }

    Ok(indexes)
}

/// Get indexes including partition indexes for schemas
/// When include_partitions is true, it will also collect indexes from partitioned table partitions
pub async fn get_indexes_in_schema_with_partitions(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: Option<&str>,
    min_size_gb: u64,
    max_size_gb: u64,
    index_type: IndexFilterType,
    order_by_size: Option<&str>,
    include_partitions: bool,
    logger: &crate::logging::Logger,
) -> Result<Vec<IndexInfo>> {
    let mut all_indexes = Vec::new();

    // If a specific table is provided, check if it's partitioned
    if let Some(table) = table_name {
        if include_partitions && is_table_partitioned(client, table, schema_name).await? {
            logger.log(
                crate::logging::LogLevel::Info,
                &format!(
                    "Table '{}.{}' is partitioned, collecting indexes from all partitions",
                    schema_name, table
                ),
            );

            // Get partition info for logging
            let partitions = get_table_partitions(client, table, schema_name).await?;
            logger.log(
                crate::logging::LogLevel::Info,
                &format!(
                    "Found {} partition(s) for table '{}.{}'",
                    partitions.len(),
                    schema_name,
                    table
                ),
            );

            // Get indexes from all partitions
            let partition_indexes = get_partitioned_table_indexes(
                client,
                table,
                schema_name,
                min_size_gb,
                max_size_gb,
            )
            .await?;

            logger.log(
                crate::logging::LogLevel::Info,
                &format!(
                    "Found {} index(es) across all partitions of '{}.{}'",
                    partition_indexes.len(),
                    schema_name,
                    table
                ),
            );

            all_indexes.extend(partition_indexes);
        } else {
            // Not partitioned or partitions not requested, get regular indexes
            let indexes = get_indexes_in_schema(
                client,
                schema_name,
                table_name,
                min_size_gb,
                max_size_gb,
                index_type,
                order_by_size,
            )
            .await?;
            all_indexes.extend(indexes);
        }
    } else {
        // No specific table, get all indexes in schema
        // First get regular indexes
        let indexes = get_indexes_in_schema(
            client,
            schema_name,
            None,
            min_size_gb,
            max_size_gb,
            index_type,
            order_by_size,
        )
        .await?;
        all_indexes.extend(indexes);

        // If include_partitions is enabled, also get indexes from partitioned tables
        if include_partitions {
            let partitioned_tables = get_partitioned_tables_in_schema(client, schema_name).await?;

            if !partitioned_tables.is_empty() {
                logger.log(
                    crate::logging::LogLevel::Info,
                    &format!(
                        "Found {} partitioned table(s) in schema '{}', collecting partition indexes",
                        partitioned_tables.len(),
                        schema_name
                    ),
                );

                for pt in &partitioned_tables {
                    logger.log(
                        crate::logging::LogLevel::Info,
                        &format!(
                            "Processing partitioned table '{}.{}' with {} partition(s)",
                            pt.schema_name, pt.table_name, pt.partition_count
                        ),
                    );

                    let partition_indexes = get_partitioned_table_indexes(
                        client,
                        &pt.table_name,
                        &pt.schema_name,
                        min_size_gb,
                        max_size_gb,
                    )
                    .await?;

                    logger.log(
                        crate::logging::LogLevel::Info,
                        &format!(
                            "Found {} index(es) across partitions of '{}.{}'",
                            partition_indexes.len(),
                            pt.schema_name,
                            pt.table_name
                        ),
                    );

                    all_indexes.extend(partition_indexes);
                }
            }
        }
    }

    // Sort by size if requested
    if let Some(order) = order_by_size {
        match order {
            "asc" => all_indexes.sort_by(|a, b| {
                a.size_bytes.unwrap_or(0).cmp(&b.size_bytes.unwrap_or(0))
            }),
            "desc" => all_indexes.sort_by(|a, b| {
                b.size_bytes.unwrap_or(0).cmp(&a.size_bytes.unwrap_or(0))
            }),
            _ => {}
        }
    }

    Ok(all_indexes)
}

/// Get indexes from multiple schemas with partition support
pub async fn get_indexes_in_schemas_with_partitions(
    client: &tokio_postgres::Client,
    schema_names: &[String],
    table_name: Option<&str>,
    min_size_gb: u64,
    max_size_gb: u64,
    index_type: IndexFilterType,
    order_by_size: Option<&str>,
    include_partitions: bool,
    logger: &crate::logging::Logger,
) -> Result<Vec<IndexInfo>> {
    let mut all_indexes = Vec::new();

    for schema_name in schema_names {
        let indexes = get_indexes_in_schema_with_partitions(
            client,
            schema_name,
            table_name,
            min_size_gb,
            max_size_gb,
            index_type,
            order_by_size,
            include_partitions,
            logger,
        )
        .await?;
        all_indexes.extend(indexes);
    }

    // Re-sort after combining all schemas if order is specified
    if let Some(order) = order_by_size {
        match order {
            "asc" => all_indexes.sort_by(|a, b| {
                a.size_bytes.unwrap_or(0).cmp(&b.size_bytes.unwrap_or(0))
            }),
            "desc" => all_indexes.sort_by(|a, b| {
                b.size_bytes.unwrap_or(0).cmp(&a.size_bytes.unwrap_or(0))
            }),
            _ => {}
        }
    }

    Ok(all_indexes)
}

pub async fn get_index_size(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
) -> Result<i64> {
    let rows = client
        .query(crate::queries::GET_INDEX_SIZE, &[&schema_name, &index_name])
        .await
        .context("Failed to query index size")?;

    if let Some(row) = rows.first() {
        let size: i64 = row.get(0);
        Ok(size)
    } else {
        Err(anyhow::anyhow!(
            "Index {}.{} not found",
            schema_name,
            index_name
        ))
    }
}

pub async fn validate_index_integrity(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
) -> Result<bool> {
    let rows = client
        .query(
            crate::queries::VALIDATE_INDEX_INTEGRITY,
            &[&schema_name, &index_name],
        )
        .await
        .context("Failed to query index integrity")?;

    if let Some(row) = rows.first() {
        let is_valid: bool = row.get(2);
        let is_ready: bool = row.get(3);
        let is_live: bool = row.get(4);

        // Index is considered healthy if it's valid, ready, and live
        Ok(is_valid && is_ready && is_live)
    } else {
        // If no rows returned, the index doesn't exist or there's an issue
        Ok(false)
    }
}

pub async fn get_index_bloat_ratio(
    client: &tokio_postgres::Client,
    index_name: &str,
) -> Result<f64> {
    let rows = client
        .query(crate::queries::GET_INDEX_BLOAT_RATIO, &[&index_name])
        .await
        .context("Failed to query index bloat ratio")?;

    if let Some(row) = rows.first() {
        // Handle numeric type conversion by getting as string and parsing
        let bloat_percentage_str: String = row.get(0);
        let bloat_percentage_f64 = bloat_percentage_str.parse::<f64>().unwrap_or(0.0);
        Ok(bloat_percentage_f64)
    } else {
        // If no rows returned, assume no bloat
        Ok(0.0)
    }
}

/// Check if an error is retryable based on error message patterns
/// Returns a tuple: (is_retryable, is_connection_error)
/// - is_retryable: true if the error should be retried
/// - is_connection_error: true if it's a connection error (needs new connection)
fn is_retryable_error(error: &tokio_postgres::Error) -> (bool, bool) {
    let error_msg = error.to_string().to_lowercase();
    
    // Check for lock timeout
    if error_msg.contains("canceling statement due to lock timeout") {
        return (true, false);
    }
    
    // Check for deadlock
    if error_msg.contains("deadlock detected") {
        return (true, false);
    }
    
    // Check for connection errors
    let connection_error_patterns = [
        "connection closed",
        "connection lost",
        "server closed the connection",
        "broken pipe",
        "connection reset",
    ];
    
    for pattern in &connection_error_patterns {
        if error_msg.contains(pattern) {
            return (true, true);
        }
    }
    
    (false, false)
}

/// Worker function that processes indexes using the memory table for concurrency control
pub async fn worker_with_memory_table(
    worker_id: usize,
    connection_string: String,
    memory_table: Arc<SharedIndexMemoryTable>,
    logger: Arc<logging::Logger>,
    config: WorkerConfig,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    logger.log(
        logging::LogLevel::Info,
        &format!("Worker {} started", worker_id),
    );

    // Create a connection for this worker
    let client = crate::connection::create_connection_with_session_parameters_ssl(
        &connection_string,
        config.maintenance_work_mem_gb,
        config.max_parallel_maintenance_workers,
        config.maintenance_io_concurrency,
        config.lock_timeout_seconds,
        config.use_ssl,
        config.accept_invalid_certs,
        config.ssl_ca_cert.clone(),
        config.ssl_client_cert.clone(),
        config.ssl_client_key.clone(),
        &logger,
    )
    .await?;

    let client = Arc::new(client);

    // Process indexes until none are available or cancellation is requested
    while memory_table.has_pending_indexes().await {
        // Check for cancellation before acquiring new index
        if *cancel_rx.borrow() {
            logger.log(
                logging::LogLevel::Info,
                &format!("Worker {} received cancellation signal, stopping...", worker_id),
            );
            break;
        }

        // Try to acquire an index
        if let Some(index_info) = memory_table
            .try_acquire_index_for_worker(worker_id, &logger)
            .await
        {
            // Check cancellation again after acquiring index
            if *cancel_rx.borrow() {
                logger.log(
                    logging::LogLevel::Info,
                    &format!(
                        "Worker {} received cancellation signal, releasing index {}.{}",
                        worker_id, index_info.schema_name, index_info.index_name
                    ),
                );
                
                // Mark index back to pending in state table if session_id is provided
                if let Some(ref _sid) = config.session_id {
                    if let Err(e) = crate::state::update_index_state(
                        &client,
                        &index_info.schema_name,
                        &index_info.index_name,
                        &crate::state::ReindexState::Pending,
                    )
                    .await
                    {
                        logger.log(
                            logging::LogLevel::Warning,
                            &format!(
                                "Failed to reset index {}.{} to pending in state table: {}",
                                index_info.schema_name, index_info.index_name, e
                            ),
                        );
                    }
                }

                // Release the index back to memory table
                memory_table
                    .release_index_and_mark_failed(
                        &index_info.schema_name,
                        &index_info.index_name,
                        &index_info.table_name,
                        worker_id,
                        &logger,
                    )
                    .await;
                
                break;
            }
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Worker {} processing index {}.{}",
                    worker_id, index_info.schema_name, index_info.index_name
                ),
            );

            // Mark index as in_progress in state table if session_id is provided
            if let Some(ref sid) = config.session_id {
                if let Err(e) = crate::state::mark_index_in_progress(
                    &client,
                    &index_info.schema_name,
                    &index_info.index_name,
                    sid,
                )
                .await
                {
                    logger.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Failed to mark index {}.{} as in_progress in state table: {}",
                            index_info.schema_name, index_info.index_name, e
                        ),
                    );
                }
            }

            // Process the index
            let result = reindex_index_with_memory_table(
                client.clone(),
                index_info.clone(),
                worker_id,
                config.skip_inactive_replication_slots,
                config.skip_sync_replication_connection,
                config.skip_active_vacuums,
                logger.clone(),
                config.bloat_threshold,
                config.concurrently,
                config.user_index_type,
                config.session_id.clone(),
                Some(connection_string.clone()),
                Some(config.clone()),
            )
            .await;

            // Handle the result
            match result {
                Ok(status) => {
                    // Update state table based on result
                    let state = match status {
                        crate::types::ReindexStatus::Success => {
                            crate::state::ReindexState::Completed
                        }
                        crate::types::ReindexStatus::Skipped
                        | crate::types::ReindexStatus::InvalidIndex
                        | crate::types::ReindexStatus::BelowBloatThreshold => {
                            crate::state::ReindexState::Skipped
                        }
                        _ => crate::state::ReindexState::Failed,
                    };

                    if let Some(ref _sid) = config.session_id {
                        if let Err(e) = crate::state::update_index_state(
                            &client,
                            &index_info.schema_name,
                            &index_info.index_name,
                            &state,
                        )
                        .await
                        {
                            logger.log(
                                logging::LogLevel::Warning,
                                &format!(
                                    "Failed to update state for index {}.{}: {}",
                                    index_info.schema_name, index_info.index_name, e
                                ),
                            );
                        }
                    }

                    match status {
                        crate::types::ReindexStatus::Success => {
                            memory_table
                                .release_index_and_mark_completed(
                                    &index_info.schema_name,
                                    &index_info.index_name,
                                    &index_info.table_name,
                                    worker_id,
                                    &logger,
                                )
                                .await;
                        }
                        crate::types::ReindexStatus::Skipped
                        | crate::types::ReindexStatus::InvalidIndex
                        | crate::types::ReindexStatus::BelowBloatThreshold => {
                            memory_table
                                .release_index_and_mark_skipped(
                                    &index_info.schema_name,
                                    &index_info.index_name,
                                    &index_info.table_name,
                                    worker_id,
                                    &logger,
                                )
                                .await;
                        }
                        _ => {
                            memory_table
                                .release_index_and_mark_failed(
                                    &index_info.schema_name,
                                    &index_info.index_name,
                                    &index_info.table_name,
                                    worker_id,
                                    &logger,
                                )
                                .await;
                        }
                    }
                },
                Err(e) => {
                    logger.log(
                        logging::LogLevel::Error,
                        &format!(
                            "Worker {} failed to process index {}.{}: {}",
                            worker_id, index_info.schema_name, index_info.index_name, e
                        ),
                    );
                    
                    // Update state table
                    if let Some(ref _sid) = config.session_id {
                        if let Err(err) = crate::state::update_index_state(
                            &client,
                            &index_info.schema_name,
                            &index_info.index_name,
                            &crate::state::ReindexState::Failed,
                        )
                        .await
                        {
                            logger.log(
                                logging::LogLevel::Warning,
                                &format!(
                                    "Failed to update state for index {}.{}: {}",
                                    index_info.schema_name, index_info.index_name, err
                                ),
                            );
                        }
                    }

                    memory_table
                        .release_index_and_mark_failed(
                            &index_info.schema_name,
                            &index_info.index_name,
                            &index_info.table_name,
                            worker_id,
                            &logger,
                        )
                        .await;
                }
            }
        } else {
            // No available indexes, wait a bit before trying again
            // Check for cancellation during wait using select
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(DEFAULT_RETRY_DELAY_MS)) => {
                    // Sleep completed, continue loop
                }
                _ = cancel_rx.changed() => {
                    // Cancellation signal received
                    if *cancel_rx.borrow() {
                        logger.log(
                            logging::LogLevel::Info,
                            &format!("Worker {} received cancellation signal during wait, stopping...", worker_id),
                        );
                        break;
                    }
                }
            }
        }
    }

    logger.log(
        logging::LogLevel::Info,
        &format!("Worker {} completed", worker_id),
    );

    Ok(())
}

/// Reindex an index using the memory table (simplified version without shared tracker)
pub async fn reindex_index_with_memory_table(
    client: Arc<tokio_postgres::Client>,
    index_info: IndexInfo,
    worker_id: usize,
    skip_inactive_replication_slots: bool,
    skip_sync_replication_connection: bool,
    skip_active_vacuums: bool,
    logger: Arc<logging::Logger>,
    bloat_threshold: Option<u8>,
    concurrently: bool,
    user_index_type: IndexFilterType,
    _session_id: Option<String>,
    connection_string: Option<String>,
    worker_config: Option<WorkerConfig>,
) -> Result<crate::types::ReindexStatus> {
    logger.log_index_start(
        worker_id,
        0, // We don't know total count in this context
        &index_info.schema_name,
        &index_info.index_name,
        &index_info.index_type,
    );

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Starting pre-reindex checks for {}.{}",
            index_info.schema_name, index_info.index_name
        ),
    );

    // Get before size
    let before_size =
        get_index_size(&client, &index_info.schema_name, &index_info.index_name).await?;
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Before size for {}.{}: {} bytes",
            index_info.schema_name, index_info.index_name, before_size
        ),
    );

    let reindex_sql = if concurrently {
        format!(
            "REINDEX INDEX CONCURRENTLY \"{}\".\"{}\"",
            index_info.schema_name, index_info.index_name
        )
    } else {
        format!(
            "REINDEX INDEX \"{}\".\"{}\"",
            index_info.schema_name, index_info.index_name
        )
    };

    // Check if the index is invalid before reindexing
    let index_is_valid =
        validate_index_integrity(&client, &index_info.schema_name, &index_info.index_name).await?;
    if !index_is_valid {
        logger.log(
            logging::LogLevel::Warning,
            &format!(
                "Index is invalid, skipping reindexing {}.{}",
                index_info.schema_name, index_info.index_name
            ),
        );
        // Save skipped record to logbook
        let index_data = crate::save::IndexData {
            schema_name: index_info.schema_name.clone(),
            index_name: index_info.index_name.clone(),
            index_type: user_index_type.to_string(),
            reindex_status: crate::types::ReindexStatus::InvalidIndex,
            before_size: None,
            after_size: None,
            size_change: None,
            reindex_duration: None,
        };
        crate::save::save_index_info(&client, &index_data).await?;
        return Ok(crate::types::ReindexStatus::InvalidIndex);
    }

    // Check bloat ratio if threshold is specified
    if let Some(threshold) = bloat_threshold {
        let bloat_ratio = get_index_bloat_ratio(&client, &index_info.index_name).await?;
        if bloat_ratio < threshold as f64 {
            logger.log(
                logging::LogLevel::Info,
                &format!(
                    "Index bloat ratio ({}%) is below threshold ({}%), skipping reindexing {}.{}",
                    bloat_ratio, threshold, index_info.schema_name, index_info.index_name
                ),
            );
            // Save skipped record to logbook
            let index_data = crate::save::IndexData {
                schema_name: index_info.schema_name.clone(),
                index_name: index_info.index_name.clone(),
                index_type: user_index_type.to_string(),
                reindex_status: crate::types::ReindexStatus::BelowBloatThreshold,
                before_size: None,
                after_size: None,
                size_change: None,
                reindex_duration: None,
            };
            crate::save::save_index_info(&client, &index_data).await?;
            return Ok(crate::types::ReindexStatus::BelowBloatThreshold);
        }
    }

    // Perform fresh reindexing checks
    let reindexing_results = crate::checks::perform_reindexing_checks(&client).await?;

    // Determine specific skip reason
    let mut skip_reasons = Vec::new();
    if reindexing_results.active_vacuum && !skip_active_vacuums {
        skip_reasons.push("active vacuum");
    }
    if reindexing_results.inactive_replication_slots && !skip_inactive_replication_slots {
        skip_reasons.push("inactive replication slots");
    }
    if reindexing_results.sync_replication_connection && !skip_sync_replication_connection {
        skip_reasons.push("sync replication connection");
    }

    if !skip_reasons.is_empty() {
        let skip_reason = skip_reasons.join(", ");
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Skipping {}.{} due to: {}",
                index_info.schema_name, index_info.index_name, skip_reason
            ),
        );

        // Save skipped record to logbook
        let index_data = crate::save::IndexData {
            schema_name: index_info.schema_name.clone(),
            index_name: index_info.index_name.clone(),
            index_type: user_index_type.to_string(),
            reindex_status: crate::types::ReindexStatus::Skipped,
            before_size: None,
            after_size: None,
            size_change: None,
            reindex_duration: None,
        };
        crate::save::save_index_info(&client, &index_data).await?;
        return Ok(crate::types::ReindexStatus::Skipped);
    }

    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Starting reindex operation for {}.{}",
            index_info.schema_name, index_info.index_name
        ),
    );

    // Clone connection parameters for use in retry loop
    let connection_string_clone = connection_string.clone();
    let worker_config_clone = worker_config.clone();
    
    let mut current_client = client.clone();
    let start_time = std::time::Instant::now();
    let mut duration = std::time::Duration::from_secs(0);
    
    // Retry loop: attempt up to 4 times (1 initial + 3 retries)
    for attempt in 0..=MAX_REINDEX_RETRIES {
        let result = current_client.execute(&reindex_sql, &[]).await;
        duration = start_time.elapsed();
        
        match result {
            Ok(_) => {
                logger.log(
                    logging::LogLevel::Info,
                    &format!(
                        "Reindex SQL executed successfully for {}.{} in {:?} (attempt {})",
                        index_info.schema_name, index_info.index_name, duration, attempt + 1
                    ),
                );
                break; // Success, exit retry loop
            }
            Err(e) => {
                let (is_retryable, is_connection_error) = is_retryable_error(&e);
                
                if !is_retryable {
                    // Non-retryable error, fail immediately
                    logger.log_index_failed(
                        &index_info.schema_name,
                        &index_info.index_name,
                        &format!("SQL execution failed after {:?} (non-retryable error): {}", duration, e),
                    );
                    
                    // Save failed reindex record
                    let index_data = crate::save::IndexData {
                        schema_name: index_info.schema_name.clone(),
                        index_name: index_info.index_name.clone(),
                        index_type: user_index_type.to_string(),
                        reindex_status: crate::types::ReindexStatus::Failed,
                        before_size: Some(before_size),
                        after_size: None,
                        size_change: None,
                        reindex_duration: Some(((duration.as_secs_f64() * 10.0).round() / 10.0) as f32),
                    };
                    crate::save::save_index_info(&current_client, &index_data).await?;
                    
                    return Ok(crate::types::ReindexStatus::Failed);
                }
                
                // Retryable error
                if attempt < MAX_REINDEX_RETRIES {
                    let error_msg_lower = e.to_string().to_lowercase();
                    let error_type = if is_connection_error {
                        "connection error"
                    } else if error_msg_lower.contains("deadlock") {
                        "deadlock"
                    } else {
                        "lock timeout"
                    };
                    
                    logger.log(
                        logging::LogLevel::Warning,
                        &format!(
                            "Reindex failed for {}.{} due to {} (attempt {}/{}): {}. Retrying in {} seconds...",
                            index_info.schema_name,
                            index_info.index_name,
                            error_type,
                            attempt + 1,
                            MAX_REINDEX_RETRIES + 1,
                            e,
                            REINDEX_RETRY_DELAY_SECS
                        ),
                    );
                    
                    // If it's a connection error, create a new connection
                    if is_connection_error {
                        if let (Some(ref conn_str), Some(ref config)) = (connection_string_clone.as_ref(), worker_config_clone.as_ref()) {
                            logger.log(
                                logging::LogLevel::Info,
                                &format!(
                                    "Creating new connection for {}.{} due to connection error",
                                    index_info.schema_name, index_info.index_name
                                ),
                            );
                            
                            match crate::connection::create_connection_with_session_parameters_ssl(
                                conn_str,
                                config.maintenance_work_mem_gb,
                                config.max_parallel_maintenance_workers,
                                config.maintenance_io_concurrency,
                                config.lock_timeout_seconds,
                                config.use_ssl,
                                config.accept_invalid_certs,
                                config.ssl_ca_cert.clone(),
                                config.ssl_client_cert.clone(),
                                config.ssl_client_key.clone(),
                                &logger,
                            )
                            .await
                            {
                                Ok(new_client) => {
                                    current_client = Arc::new(new_client);
                                    logger.log(
                                        logging::LogLevel::Info,
                                        &format!(
                                            "Successfully created new connection for {}.{}",
                                            index_info.schema_name, index_info.index_name
                                        ),
                                    );
                                }
                                Err(conn_err) => {
                                    logger.log(
                                        logging::LogLevel::Error,
                                        &format!(
                                            "Failed to create new connection for {}.{}: {}",
                                            index_info.schema_name, index_info.index_name, conn_err
                                        ),
                                    );
                                    // Continue with existing client, will likely fail but we'll try
                                }
                            }
                        } else {
                            logger.log(
                                logging::LogLevel::Warning,
                                &format!(
                                    "Connection error detected for {}.{} but connection creation parameters not provided. Attempting retry with existing connection.",
                                    index_info.schema_name, index_info.index_name
                                ),
                            );
                        }
                    }
                    
                    // Wait before retrying
                    tokio::time::sleep(tokio::time::Duration::from_secs(REINDEX_RETRY_DELAY_SECS)).await;
                } else {
                    // All retries exhausted
                    logger.log_index_failed(
                        &index_info.schema_name,
                        &index_info.index_name,
                        &format!("SQL execution failed after {:?} and {} retry attempts: {}", duration, MAX_REINDEX_RETRIES, e),
                    );
                    
                    // Save failed reindex record
                    let index_data = crate::save::IndexData {
                        schema_name: index_info.schema_name.clone(),
                        index_name: index_info.index_name.clone(),
                        index_type: user_index_type.to_string(),
                        reindex_status: crate::types::ReindexStatus::Failed,
                        before_size: Some(before_size),
                        after_size: None,
                        size_change: None,
                        reindex_duration: Some(((duration.as_secs_f64() * 10.0).round() / 10.0) as f32),
                    };
                    crate::save::save_index_info(&current_client, &index_data).await?;
                    
                    return Ok(crate::types::ReindexStatus::Failed);
                }
            }
        }
    }
    
    // If we get here, the reindex succeeded (we broke out of the loop)
    // Update client reference for subsequent operations
    let client = current_client;

    // Get after size
    let after_size =
        get_index_size(&client, &index_info.schema_name, &index_info.index_name).await?;

    // Calculate percentage reduction
    let percentage_reduction = if before_size > 0 {
        ((before_size - after_size) as f64 / before_size as f64) * 100.0
    } else {
        0.0
    };
    // Log after size with percentage reduction
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "After size for {}.{}: {} bytes, {:.1}% reduced",
            index_info.schema_name, index_info.index_name, after_size, percentage_reduction
        ),
    );
    // Final validation
    let index_is_valid =
        validate_index_integrity(&client, &index_info.schema_name, &index_info.index_name).await?;
    if !index_is_valid {
        logger.log_index_validation_failed(&index_info.schema_name, &index_info.index_name);

        // Save failed validation record
        let index_data = crate::save::IndexData {
            schema_name: index_info.schema_name.clone(),
            index_name: index_info.index_name.clone(),
            index_type: user_index_type.to_string(),
            reindex_status: crate::types::ReindexStatus::ValidationFailed,
            before_size: Some(before_size),
            after_size: Some(after_size),
            size_change: Some(((percentage_reduction * 10.0).round() / 10.0) as f32),
            reindex_duration: Some(((duration.as_secs_f64() * 10.0).round() / 10.0) as f32),
        };
        crate::save::save_index_info(&client, &index_data).await?;
        return Ok(crate::types::ReindexStatus::ValidationFailed);
    }

    // Save success record
    let index_data = crate::save::IndexData {
        schema_name: index_info.schema_name.clone(),
        index_name: index_info.index_name.clone(),
        index_type: user_index_type.to_string(),
        reindex_status: crate::types::ReindexStatus::Success,
        before_size: Some(before_size),
        after_size: Some(after_size),
        size_change: Some(percentage_reduction as f32),
        reindex_duration: Some(((duration.as_secs_f64() * 10.0).round() / 10.0) as f32),
    };
    crate::save::save_index_info(&client, &index_data).await?;

    logger.log_index_success(&index_info.schema_name, &index_info.index_name);
    Ok(crate::types::ReindexStatus::Success)
}

/// Drop a single orphaned _ccnew index after validating it's safe to drop
pub async fn clean_orphaned_ccnew_index(
    client: &tokio_postgres::Client,
    schema_name: &str,
    index_name: &str,
    logger: &logging::Logger,
) -> Result<()> {
    // First validate the index integrity to ensure it's safe to drop
    logger.log(
        logging::LogLevel::Info,
        &format!(
            "Validating index integrity before dropping orphaned index: {}.{}",
            schema_name, index_name
        ),
    );

    let index_is_valid = validate_index_integrity(client, schema_name, index_name).await?;

    if !index_is_valid {
        logger.log(
            logging::LogLevel::Warning,
            &format!(
                "Index {}.{} is invalid, proceeding with drop operation",
                schema_name, index_name
            ),
        );
    } else {
        logger.log(
            logging::LogLevel::Info,
            &format!(
                "Index {}.{} is valid, proceeding with drop operation",
                schema_name, index_name
            ),
        );
    }

    let drop_sql = format!(
        "DROP INDEX IF EXISTS \"{}\".\"{}\"",
        schema_name, index_name
    );

    logger.log(
        logging::LogLevel::Info,
        &format!("Dropping orphaned index: {}.{}", schema_name, index_name),
    );

    match client.execute(&drop_sql, &[]).await {
        Ok(_) => {
            logger.log(
                logging::LogLevel::Success,
                &format!(
                    "Successfully dropped orphaned index: {}.{}",
                    schema_name, index_name
                ),
            );
            Ok(())
        }
        Err(e) => {
            logger.log(
                logging::LogLevel::Error,
                &format!(
                    "Failed to drop orphaned index {}.{}: {}",
                    schema_name, index_name, e
                ),
            );
            Err(anyhow::anyhow!(
                "Failed to drop orphaned index {}.{}: {}",
                schema_name,
                index_name,
                e
            ))
        }
    }
}

/// Save excluded indexes to logbook before filtering them out
pub async fn save_excluded_indexes_to_logbook(
    client: &tokio_postgres::Client,
    indexes: &[IndexInfo],
    excluded_indexes: &std::collections::HashSet<String>,
    index_type: IndexFilterType,
    logger: &logging::Logger,
) -> Result<()> {
    for index in indexes {
        if excluded_indexes.contains(&index.index_name) {
            logger.log(
                logging::LogLevel::Info,
                &format!("Excluding index from reindexing: {}", index.index_name),
            );

            // Save excluded index to logbook
            let index_data = crate::save::IndexData {
                schema_name: index.schema_name.clone(),
                index_name: index.index_name.clone(),
                index_type: index_type.to_string(),
                reindex_status: crate::types::ReindexStatus::Excluded,
                before_size: None,
                after_size: None,
                size_change: None,
                reindex_duration: None,
            };

            if let Err(e) = crate::save::save_index_info(client, &index_data).await {
                logger.log(
                    logging::LogLevel::Error,
                    &format!(
                        "Failed to save excluded index info for {}.{}: {}",
                        index.schema_name, index.index_name, e
                    ),
                );
            }
        }
    }
    Ok(())
}

/// Filter out excluded indexes and orphaned _ccnew indexes from processing
pub fn filter_indexes(
    indexes: Vec<IndexInfo>,
    excluded_indexes: &std::collections::HashSet<String>,
    logger: &logging::Logger,
) -> Vec<IndexInfo> {
    indexes
        .into_iter()
        .filter(|index| {
            // Check if index is in exclude list
            if excluded_indexes.contains(&index.index_name) {
                return false;
            }

            // Check if index is a temporary concurrent reindex index
            if is_temporary_concurrent_reindex_index(&index.index_name) {
                logger.log(
                    logging::LogLevel::Warning,
                    &format!(
                        "Index appears to be a temporary concurrent reindex index (matches '_ccnew' pattern). Skipping reindexing: {}",
                        index.index_name
                    ),
                );
                return false;
            }

            true
        })
        .collect()
}
